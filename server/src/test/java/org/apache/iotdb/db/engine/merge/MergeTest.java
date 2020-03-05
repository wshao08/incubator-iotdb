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

package org.apache.iotdb.db.engine.merge;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;

abstract class MergeTest {

  static final String MERGE_TEST_SG = "root.mergeTest";

  int seqFileNum = 5;
  int unseqFileNum = 5;
  int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private int prevMergeChunkThreshold;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException, MetadataException {
    MManager.getInstance().init();
    prevMergeChunkThreshold =
        IoTDBDescriptor.getInstance().getConfig().getChunkMergePointThreshold();
    IoTDBDescriptor.getInstance().getConfig().setChunkMergePointThreshold(-1);
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
    MergeManager.getINSTANCE().start();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    IoTDBDescriptor.getInstance().getConfig().setChunkMergePointThreshold(prevMergeChunkThreshold);
    TsFileMetaDataCache.getInstance().clear();
    DeviceMetaDataCache.getInstance().clear();
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
    MergeManager.getINSTANCE().stop();
  }

  private void prepareSeries() throws MetadataException, MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] = new MeasurementSchema("sensor" + i, TSDataType.DOUBLE,
          encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = MERGE_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    MManager.getInstance().setStorageGroup(MERGE_TEST_SG);
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        MManager.getInstance().createTimeseries(
            device + PATH_SEPARATOR + measurementSchema.getMeasurementId(), measurementSchema
                .getType(), measurementSchema.getEncodingType(), measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  void prepareFiles(int seqFileNum, int unseqFileNum)
      throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
          i + "seq" + IoTDBConstant.TSFILE_NAME_SEPARATOR + i + IoTDBConstant.TSFILE_NAME_SEPARATOR
              + i + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0
              + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setHistoricalVersions(Collections.singleton((long) i));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
          i + "unseq" + IoTDBConstant.TSFILE_NAME_SEPARATOR
              + i + IoTDBConstant.TSFILE_NAME_SEPARATOR
              + i + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0
              + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setHistoricalVersions(Collections.singleton((long) (i + seqFileNum)));
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    File file = new File(TestConstant.BASE_OUTPUT_PATH
        .concat(unseqFileNum + "unseq" + IoTDBConstant.TSFILE_NAME_SEPARATOR + unseqFileNum
            + IoTDBConstant.TSFILE_NAME_SEPARATOR + unseqFileNum
            + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0 + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.setHistoricalVersions(Collections.singleton((long) (seqFileNum + unseqFileNum)));
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * unseqFileNum, 20000);
  }

  private void removeFiles() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      tsFileResource.remove();
    }
    for (TsFileResource tsFileResource : unseqResources) {
      tsFileResource.remove();
    }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    FileReaderManager.getInstance().stop();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum,
      long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getFile());
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      fileWriter.addMeasurement(measurementSchema);
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(DataPoint.getDataPoint(measurementSchemas[k].getType(),
              measurementSchemas[k].getMeasurementId(), String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceIds[j], i);
        tsFileResource.updateEndTime(deviceIds[j], i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushForTest();
      }
    }
    fileWriter.close();
  }
}