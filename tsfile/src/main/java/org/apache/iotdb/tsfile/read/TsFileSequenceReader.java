/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.exception.NotCompatibleException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.tsfile.write.writer.TsFileIOWriter.magicStringBytes;

public class TsFileSequenceReader implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSequenceReader.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  protected String file;
  private TsFileInput tsFileInput;
  private long fileMetadataPos;
  private int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  private int totalChunkNum;
  private TsFileMetaData tsFileMetaData;
  private EndianType endianType = EndianType.BIG_ENDIAN;
  private boolean isOldVersion = false;

  private boolean cacheDeviceMetadata = false;
  private Map<TsDeviceMetadataIndex, TsDeviceMetadata> deviceMetadataMap;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.getBytes().length
   * + TSFileConfig.NUMBER_VERSION.getBytes().length bytes of the file for preparing reading real
   * data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileSequenceReader(String file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
    if (resourceLogger.isInfoEnabled()) {
      resourceLogger.info("{} reader is opened. {}", file, getClass().getName());
    }
    this.file = file;
    tsFileInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(file);
    // old version number of TsFile using little endian starts with "v"
    this.endianType = this.readVersionNumber().startsWith("v")
        ? EndianType.LITTLE_ENDIAN : EndianType.BIG_ENDIAN;
    this.isOldVersion = this.readVersionNumber().startsWith("v");
    try {
      if (loadMetadataSize) {
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  public TsFileSequenceReader(String file, boolean loadMetadata, boolean cacheDeviceMetadata)
      throws IOException {
    this(file, loadMetadata);
    this.cacheDeviceMetadata = cacheDeviceMetadata;
    if (cacheDeviceMetadata) {
      deviceMetadataMap = new ConcurrentHashMap<>();
    }
  }

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first TSFileConfig.MAGIC_STRING.getBytes().length
   * + TSFileConfig.NUMBER_VERSION.getBytes().length bytes of the file for preparing reading real
   * data.
   *
   * @param input given input
   */
  public TsFileSequenceReader(TsFileInput input) throws IOException {
    this(input, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReader(TsFileInput input, boolean loadMetadataSize)
      throws IOException {
    this.tsFileInput = input;
    try {
      if (loadMetadataSize) { // NOTE no autoRepair here
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input the input of a tsfile. The current position should be a markder and then a chunk
   * Header, rather than the magic number
   * @param fileMetadataPos the position of the file metadata in the TsFileInput from the beginning
   * of the input to the current position
   * @param fileMetadataSize the byte size of the file metadata in the input
   */
  public TsFileSequenceReader(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
    this.tsFileInput = input;
    this.fileMetadataPos = fileMetadataPos;
    this.fileMetadataSize = fileMetadataSize;
  }

  public void loadMetadataSize() throws IOException {
    ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
    if (readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      tsFileInput.read(metadataSize,
          tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES);
      metadataSize.flip();
      // read file metadata size and position
      fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
      fileMetadataPos =
          tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES
              - fileMetadataSize;
    } else if (readTailMagic().equals(TSFileConfig.OLD_VERSION)) {
      tsFileInput.read(metadataSize,
          tsFileInput.size() - TSFileConfig.OLD_MAGIC_STRING.getBytes().length - Integer.BYTES);
      metadataSize.flip();
      // read file metadata size and position
      fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
      fileMetadataPos =
          tsFileInput.size() - TSFileConfig.OLD_MAGIC_STRING.getBytes().length - Integer.BYTES
              - fileMetadataSize;
    }
  }

  public long getFileMetadataPos() {
    return fileMetadataPos;
  }

  public int getFileMetadataSize() {
    return fileMetadataSize;
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public String readTailMagic() throws IOException {
    long totalSize = tsFileInput.size();
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.getBytes().length);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * whether the file is a complete TsFile: only if the head magic and tail magic string exists.
   */
  public boolean isComplete() throws IOException {
    return tsFileInput.size()
        >= TSFileConfig.MAGIC_STRING.getBytes().length * 2 + TSFileConfig.VERSION_NUMBER.getBytes().length &&
        (readTailMagic().equals(readHeadMagic()) || readTailMagic().equals(TSFileConfig.OLD_VERSION));
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public String readHeadMagic() throws IOException {
    return readHeadMagic(false);
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @param movePosition whether move the position of the file reader after reading the magic header
   * to the end of the magic head string.
   */
  public String readHeadMagic(boolean movePosition) throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    if (movePosition) {
      tsFileInput.position(0);
      tsFileInput.read(magicStringBytes);
    } else {
      tsFileInput.read(magicStringBytes, 0);
    }
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /**
   * this function reads version number and checks compatibility of TsFile.
   */
  public String readVersionNumber() throws IOException, NotCompatibleException {
    ByteBuffer versionNumberBytes = ByteBuffer
        .allocate(TSFileConfig.VERSION_NUMBER.getBytes().length);
    tsFileInput.read(versionNumberBytes, TSFileConfig.MAGIC_STRING.getBytes().length);
    versionNumberBytes.flip();
    return new String(versionNumberBytes.array());
  }

  public EndianType getEndianType() {
    return this.endianType;
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public TsFileMetaData readFileMetadata() throws IOException {
    if (tsFileMetaData == null) {
      tsFileMetaData = TsFileMetaData
          .deserializeFrom(readData(fileMetadataPos, fileMetadataSize), isOldVersion);
    }
    if (isOldVersion) {
      tsFileMetaData.setTotalChunkNum(countTotalChunkNum());
    }
    return tsFileMetaData;
  }

  /**
   * count total chunk num
   */
  private int countTotalChunkNum() throws IOException {
    int count = 0;
    for (TsDeviceMetadataIndex deviceIndex : tsFileMetaData.getDeviceMap().values()) {
      TsDeviceMetadata deviceMetadata = readTsDeviceMetaData(deviceIndex);
      for (ChunkGroupMetaData chunkGroupMetaData : deviceMetadata
          .getChunkGroupMetaDataList()) {
        count += chunkGroupMetaData.getChunkMetaDataList().size();
      }
    }
    return count;
  }

  /**
   * this function does not modify the position of the file reader.
   */
  public TsDeviceMetadata readTsDeviceMetaData(TsDeviceMetadataIndex index) throws IOException {
    if (index == null) {
      return null;
    }
    TsDeviceMetadata deviceMetadata = null;
    if (cacheDeviceMetadata) {
      deviceMetadata = deviceMetadataMap.get(index);
    }
    if (deviceMetadata == null) {
      deviceMetadata = TsDeviceMetadata.deserializeFrom(readData(index.getOffset(), index.getLen()));
      if (cacheDeviceMetadata) {
        deviceMetadataMap.put(index, deviceMetadata);
      }
    }
    return deviceMetadata;
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupFooter readChunkGroupFooter() throws IOException {
    return ChunkGroupFooter.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER.
   *
   * @param position the offset of the chunk group footer in the file
   * @param markerRead true if the offset does not contains the marker , otherwise false
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupFooter readChunkGroupFooter(long position, boolean markerRead)
      throws IOException {
    return ChunkGroupFooter.deserializeFrom(tsFileInput, position, markerRead);
  }

  /**
   * After reading the footer of a ChunkGroup, call this method to set the file pointer to the start
   * of the data of this ChunkGroup if you want to read its data next. <br> This method is not
   * threadsafe.
   *
   * @param footer the chunkGroupFooter which you want to read data
   */
  public void setPositionToAChunkGroup(ChunkGroupFooter footer) throws IOException {
    tsFileInput
        .position(tsFileInput.position() - footer.getDataSize() - footer.getSerializedSize());
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br> This
   * method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader() throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read the chunk's header.
   *
   * @param position the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead true if the offset does not contains the marker , otherwise false
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize, boolean markerRead)
      throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position, chunkHeaderSize, markerRead);
  }

  /**
   * notice, the position of the channel MUST be at the end of this header. <br> This method is not
   * threadsafe.
   *
   * @return the pages of this chunk
   */
  public ByteBuffer readChunk(ChunkHeader header) throws IOException {
    return readData(-1, header.getDataSize());
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  public ByteBuffer readChunk(ChunkHeader header, long position) throws IOException {
    return readData(position, header.getDataSize());
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param dataSize the size of chunkdata
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  private ByteBuffer readChunk(long position, int dataSize) throws IOException {
    return readData(position, dataSize);
  }

  /**
   * read memory chunk.
   *
   * @param metaData -given chunk meta data
   * @return -chunk
   */
  public Chunk readMemChunk(ChunkMetaData metaData) throws IOException {
    int chunkHeadSize = ChunkHeader.getSerializedSize(metaData.getMeasurementUid());
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize, false);
    ByteBuffer buffer = readChunk(metaData.getOffsetOfChunkHeader() + header.getSerializedSize(),
        header.getDataSize());
    return new Chunk(header, buffer, metaData.getDeletedAt(), endianType);
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type);
  }

  public long position() throws IOException {
    return tsFileInput.position();
  }

  public void position(long offset) throws IOException {
    tsFileInput.position(offset);
  }

  public void skipPageData(PageHeader header) throws IOException {
    tsFileInput.position(tsFileInput.position() + header.getCompressedSize());
  }

  public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
    return readPage(header, type, -1);
  }

  private ByteBuffer readPage(PageHeader header, CompressionType type, long position)
      throws IOException {
    ByteBuffer buffer = readData(position, header.getCompressedSize());
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(type);
    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
    if (type == CompressionType.UNCOMPRESSED) {
      return buffer;
    }// FIXME if the buffer is not array-implemented.
    unCompressor.uncompress(buffer.array(), buffer.position(), buffer.remaining(),
        uncompressedBuffer.array(),
        0);
    return uncompressedBuffer;
  }

  /**
   * read one byte from the input. <br> this method is not thread safe
   */
  public byte readMarker() throws IOException {
    markerBuffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, markerBuffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    markerBuffer.flip();
    return markerBuffer.get();
  }

  public byte readMarker(long position) throws IOException {
    return readData(position, Byte.BYTES).get();
  }

  public void close() throws IOException {
    if (resourceLogger.isInfoEnabled()) {
      resourceLogger.info("{} reader is closed.", file);
    }
    this.tsFileInput.close();
    deviceMetadataMap = null;
  }

  public String getFileName() {
    return this.file;
  }

  public long fileSize() throws IOException {
    return tsFileInput.size();
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position. <br> if position = -1, the tsFileInput's position will be changed to the current
   * position + real data size that been read. Other wise, the tsFileInput's position is not
   * changed.
   *
   * @param position the start position of data in the tsFileInput, or the current position if
   * position = -1
   * @param size the size of data that want to read
   * @return data that been read.
   */
  private ByteBuffer readData(long position, int size) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    if (position == -1) {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) != size) {
        throw new IOException("reach the end of the data");
      }
    } else {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer, position, size) != size) {
        throw new IOException("reach the end of the data");
      }
    }
    buffer.flip();
    return buffer;
  }

  /**
   * notice, the target bytebuffer are not flipped.
   */
  public int readRaw(long position, int length, ByteBuffer target) throws IOException {
    return ReadWriteIOUtils
        .readAsPossible(tsFileInput, target, position, length);
  }

  /**
   * Self Check the file and return the position before where the data is safe.
   *
   * @param newSchema @OUT.  the measurement schema in the file will be added into this parameter.
   * (can be null)
   * @param newMetaData @OUT can not be null, the chunk group metadta in the file will be added into
   * this parameter.
   * @param fastFinish if true and the file is complete, then newSchema and newMetaData parameter
   * will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   * be truncated.
   */
  public long selfCheck(Map<String, MeasurementSchema> newSchema,
      List<ChunkGroupMetaData> newMetaData, boolean fastFinish) throws IOException {
    File checkFile = FSFactoryProducer.getFSFactory().getFile(this.file);
    long fileSize;
    if (!checkFile.exists()) {
      return TsFileCheckStatus.FILE_NOT_FOUND;
    } else {
      fileSize = checkFile.length();
    }
    ChunkMetaData currentChunk;
    String measurementID;
    TSDataType dataType;
    long fileOffsetOfChunk;

    ChunkGroupMetaData currentChunkGroup;
    List<ChunkMetaData> chunks = null;
    String deviceID;
    long startOffsetOfChunkGroup = 0;
    long endOffsetOfChunkGroup;
    long versionOfChunkGroup = 0;

    if (fileSize < TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    String magic = readHeadMagic(true);
    tsFileInput.position(TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length);
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    if (fileSize == TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER
        .getBytes().length) {
      return TsFileCheckStatus.ONLY_MAGIC_HEAD;
    } else if (readTailMagic().equals(magic)) {
      loadMetadataSize();
      if (fastFinish) {
        return TsFileCheckStatus.COMPLETE_FILE;
      }
    }
    boolean newChunkGroup = true;
    // not a complete file, we will recover it...
    long truncatedPosition = magicStringBytes.length;
    boolean goon = true;
    byte marker;
    int chunkCnt = 0;
    try {
      while (goon && (marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            // this is the first chunk of a new ChunkGroup.
            if (newChunkGroup) {
              newChunkGroup = false;
              chunks = new ArrayList<>();
              startOffsetOfChunkGroup = this.position() - 1;
            }
            fileOffsetOfChunk = this.position() - 1;
            // if there is something wrong with a chunk, we will drop the whole ChunkGroup
            // as different chunks may be created by the same insertions(sqls), and partial
            // insertion is not tolerable
            ChunkHeader header = this.readChunkHeader();
            measurementID = header.getMeasurementID();
            if (newSchema != null) {
              newSchema.putIfAbsent(measurementID,
                  new MeasurementSchema(measurementID, header.getDataType(),
                      header.getEncodingType(), header.getCompressionType()));
            }
            dataType = header.getDataType();
            Statistics<?> chunkStatistics = Statistics.getStatsByType(dataType);
            if (header.getNumOfPages() > 0) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              this.skipPageData(pageHeader);
            }
            for (int j = 1; j < header.getNumOfPages() - 1; j++) {
              //a new Page
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              this.skipPageData(pageHeader);
            }
            if (header.getNumOfPages() > 1) {
              PageHeader pageHeader = this.readPageHeader(header.getDataType());
              chunkStatistics.mergeStatistics(pageHeader.getStatistics());
              this.skipPageData(pageHeader);
            }
            currentChunk = new ChunkMetaData(measurementID, dataType, fileOffsetOfChunk,
                 chunkStatistics);
            chunks.add(currentChunk);
            chunkCnt++;
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            //this is a chunk group
            //if there is something wrong with the ChunkGroup Footer, we will drop this ChunkGroup
            //because we can not guarantee the correctness of the deviceId.
            ChunkGroupFooter chunkGroupFooter = this.readChunkGroupFooter();
            deviceID = chunkGroupFooter.getDeviceID();
            endOffsetOfChunkGroup = this.position();
            currentChunkGroup = new ChunkGroupMetaData(deviceID, chunks, startOffsetOfChunkGroup);
            currentChunkGroup.setEndOffsetOfChunkGroup(endOffsetOfChunkGroup);
            currentChunkGroup.setVersion(versionOfChunkGroup++);
            newMetaData.add(currentChunkGroup);
            newChunkGroup = true;
            truncatedPosition = this.position();

            totalChunkNum += chunkCnt;
            chunkCnt = 0;
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            MetaMarker.handleUnexpectedMarker(marker);
            goon = false;
            logger.error(String
                .format("Unrecognized marker detected, this file {%s} may be corrupted", file));
        }
      }
      // now we read the tail of the data section, so we are sure that the last ChunkGroupFooter is
      // complete.
      truncatedPosition = this.position() - 1;
    } catch (Exception e2) {
      logger.info("TsFile {} self-check cannot proceed at position {} after {} chunk groups "
          + "recovered, because : {}", file, this.position(), newMetaData.size(), e2.getMessage());
    }
    // Despite the completeness of the data section, we will discard current FileMetadata
    // so that we can continue to write data into this tsfile.
    return truncatedPosition;
  }

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  public List<ChunkMetaData> getChunkMetadataList(Path path) throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    if (!tsFileMetaData.containsDevice(path.getDevice())) {
      return new ArrayList<>();
    }

    // get the index information of TsDeviceMetadata
    TsDeviceMetadataIndex index = tsFileMetaData.getDeviceMetadataIndex(path.getDevice());

    // read TsDeviceMetadata from file
    TsDeviceMetadata tsDeviceMetadata = readTsDeviceMetaData(index);

    // get all ChunkMetaData of this path included in all ChunkGroups of this device
    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
    for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroupMetaDataList()) {
      List<ChunkMetaData> chunkMetaDataListInOneChunkGroup = chunkGroupMetaData
          .getChunkMetaDataList();
      for (ChunkMetaData chunkMetaData : chunkMetaDataListInOneChunkGroup) {
        if (path.getMeasurement().equals(chunkMetaData.getMeasurementUid())) {
          chunkMetaData.setVersion(chunkGroupMetaData.getVersion());
          chunkMetaDataList.add(chunkMetaData);
        }
      }
    }
    chunkMetaDataList.sort(Comparator.comparingLong(ChunkMetaData::getStartTime));
    return chunkMetaDataList;
  }

  public List<ChunkGroupMetaData> getSortedChunkGroupMetaDataListByDeviceIds() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }

    List<ChunkGroupMetaData> result = new ArrayList<>();

    for (Map.Entry<String, TsDeviceMetadataIndex> entry : tsFileMetaData.getDeviceMap()
        .entrySet()) {
      // read TsDeviceMetadata from file
      TsDeviceMetadata tsDeviceMetadata = readTsDeviceMetaData(entry.getValue());
      result.addAll(tsDeviceMetadata.getChunkGroupMetaDataList());
    }
    // sort by the start offset Of the ChunkGroup
    result.sort(Comparator.comparingLong(ChunkGroupMetaData::getStartOffsetOfChunkGroup));

    return result;
  }

  /**
   * get device names in range
   *
   * @param start start of the file
   * @param end end of the file
   * @return device names in range
   */
  public List<String> getDeviceNameInRange(long start, long end) {
    List<String> res = new ArrayList<>();

    try {
      TsFileMetaData tsFileMetaData = readFileMetadata();
      for (Map.Entry<String, TsDeviceMetadataIndex> entry : tsFileMetaData.getDeviceMap()
          .entrySet()) {
        TsDeviceMetadata tsDeviceMetadata = readTsDeviceMetaData(entry.getValue());
        for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata
            .getChunkGroupMetaDataList()) {
          LocateStatus mode = checkLocateStatus(chunkGroupMetaData, start,
              end);
          if (mode == LocateStatus.in) {
            res.add(entry.getKey());
            break;
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return res;
  }

  /**
   * Check the location of a given chunkGroupMetaData with respect to a space partition constraint.
   *
   * @param chunkGroupMetaData the given chunkGroupMetaData
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos the end position of the space partition
   * @return LocateStatus
   */
  private LocateStatus checkLocateStatus(ChunkGroupMetaData chunkGroupMetaData,
      long spacePartitionStartPos, long spacePartitionEndPos) {
    long startOffsetOfChunkGroup = chunkGroupMetaData.getStartOffsetOfChunkGroup();
    long endOffsetOfChunkGroup = chunkGroupMetaData.getEndOffsetOfChunkGroup();
    long middleOffsetOfChunkGroup = (startOffsetOfChunkGroup + endOffsetOfChunkGroup) / 2;
    if (spacePartitionStartPos <= middleOffsetOfChunkGroup
        && middleOffsetOfChunkGroup < spacePartitionEndPos) {
      return LocateStatus.in;
    } else if (middleOffsetOfChunkGroup < spacePartitionStartPos) {
      return LocateStatus.before;
    } else {
      return LocateStatus.after;
    }
  }

  /**
   * The location of a chunkGroupMetaData with respect to a space partition constraint.
   * <p>
   * in - the middle point of the chunkGroupMetaData is located in the current space partition.
   * before - the middle point of the chunkGroupMetaData is located before the current space
   * partition. after - the middle point of the chunkGroupMetaData is located after the current
   * space partition.
   */
  private enum LocateStatus {
    in, before, after
  }
}
