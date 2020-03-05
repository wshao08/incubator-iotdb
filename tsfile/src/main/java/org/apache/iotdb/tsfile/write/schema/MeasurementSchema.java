/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.write.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * This class describes a measurement's information registered in {@linkplain Schema FilSchema},
 * including measurement id, data type, encoding and compressor type. For each TSEncoding,
 * MeasurementSchema maintains respective TSEncodingBuilder; For TSDataType, only ENUM has
 * TSDataTypeConverter up to now.
 */
public class MeasurementSchema implements Comparable<MeasurementSchema>, Serializable {

  private TSDataType type;
  private TSEncoding encoding;
  private String measurementId;
  //TODO serializable interface may serialize this field. So it is time to
  // improve how to serialize MGraph in MManager.
  private TSEncodingBuilder encodingConverter;
  private CompressionType compressor;
  private Map<String, String> props = new HashMap<>();

  public MeasurementSchema() {
  }

  /**
   * set properties as an empty Map.
   */
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding) {
    this(measurementId, type, encoding,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding,
      CompressionType compressionType) {
    this(measurementId, type, encoding, compressionType, Collections.emptyMap());
  }

  /**
   * Constructor of MeasurementSchema.
   *
   * <p>props - information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN,
   * Encoder.maxStringLength
   */
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding,
      CompressionType compressionType, Map<String, String> props) {
    this.type = type;
    this.measurementId = measurementId;
    this.encoding = encoding;
    this.props = props == null ? Collections.emptyMap() : props;
    this.compressor = compressionType;
  }

  /**
   * function for deserializing data from input stream.
   */
  public static MeasurementSchema deserializeFrom(InputStream inputStream) throws IOException {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(inputStream);

    measurementSchema.type = ReadWriteIOUtils.readDataType(inputStream);

    measurementSchema.encoding = ReadWriteIOUtils.readEncoding(inputStream);

    measurementSchema.compressor = ReadWriteIOUtils.readCompressionType(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(inputStream);
        value = ReadWriteIOUtils.readString(inputStream);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  /**
   * function for deserializing data from byte buffer.
   */
  public static MeasurementSchema deserializeFrom(ByteBuffer buffer) {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(buffer);

    measurementSchema.type = ReadWriteIOUtils.readDataType(buffer);

    measurementSchema.encoding = ReadWriteIOUtils.readEncoding(buffer);

    measurementSchema.compressor = ReadWriteIOUtils.readCompressionType(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = ReadWriteIOUtils.readString(buffer);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public TSEncoding getEncodingType() {
    return encoding;
  }

  public TSDataType getType() {
    return type;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  /**
   * return the max possible length of given type.
   *
   * @return length in unit of byte
   */
  public int getTypeLength() {
    switch (type) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 4;
      case INT64:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case TEXT:
        // 4 is the length of string in type of Integer.
        // Note that one char corresponding to 3 byte is valid only in 16-bit BMP
        return TSFileDescriptor.getInstance().getConfig().getMaxStringLength() * TSFileConfig.BYTE_SIZE_PER_CHAR + 4;
      default:
        throw new UnSupportedDataTypeException(type.toString());
    }
  }

  /**
   * function for getting time encoder.
   * TODO can I be optimized?
   */
  public Encoder getTimeEncoder() {
    TSEncoding timeSeriesEncoder = TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSDataType.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType());
    return TSEncodingBuilder.getConverter(timeSeriesEncoder).getEncoder(timeType);
  }

  /**
   * get Encoder of value from encodingConverter by measurementID and data type.
   * TODO can I be optimized?
   * @return Encoder for value
   */
  public Encoder getValueEncoder() {
    //it is ok even if encodingConverter is constructed two instances for concurrent scenario..
    if (encodingConverter == null) {
      // initialize TSEncoding. e.g. set max error for PLA and SDT
      encodingConverter = TSEncodingBuilder.getConverter(encoding);
      encodingConverter.initFromProps(props);
    }
    return encodingConverter.getEncoder(type);
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  /**
   * function for serializing data to output stream.
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);

    byteLen += ReadWriteIOUtils.write(type, outputStream);

    byteLen += ReadWriteIOUtils.write(encoding, outputStream);

    byteLen += ReadWriteIOUtils.write(compressor, outputStream);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), outputStream);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    }

    return byteLen;
  }

  /**
   * function for serializing data to byte buffer.
   */
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, buffer);

    byteLen += ReadWriteIOUtils.write(type, buffer);

    byteLen += ReadWriteIOUtils.write(encoding, buffer);

    byteLen += ReadWriteIOUtils.write(compressor, buffer);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, buffer);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), buffer);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), buffer);
      }
    }

    return byteLen;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MeasurementSchema that = (MeasurementSchema) o;
    return type == that.type && encoding == that.encoding && Objects
        .equals(measurementId, that.measurementId)
        && Objects.equals(compressor, that.compressor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, encoding, measurementId, compressor);
  }

  /**
   * compare by measurementID.
   */
  @Override
  public int compareTo(MeasurementSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.measurementId.compareTo(o.measurementId);
    }
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    sc.addTail("[", measurementId, ",", type.toString(), ",", encoding.toString(), ",",
        props.toString(), ",",
        compressor.toString());
    sc.addTail("]");
    return sc.toString();
  }

}
