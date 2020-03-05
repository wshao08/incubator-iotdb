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

package org.apache.iotdb.db.engine.modification;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import junit.framework.TestCase;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.io.LocalTextModificationAccessor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeletionFileNodeTest {

  private String processorName = "root.test";

  private static String[] measurements = new String[10];
  private String dataType = TSDataType.DOUBLE.toString();
  private String encoding = TSEncoding.PLAIN.toString();

  static {
    for (int i = 0; i < 10; i++) {
      measurements[i] = "m" + i;
    }
  }

  @Before
  public void setup() throws MetadataException, StorageEngineException, StartupException {
    EnvironmentUtils.envSetUp();

    MManager.getInstance().setStorageGroup(processorName);
    for (int i = 0; i < 10; i++) {
      MManager.getInstance().createTimeseries(processorName + "." + measurements[i], dataType,
          encoding);
      StorageEngine.getInstance()
          .addTimeSeries(new Path(processorName, measurements[i]), TSDataType.valueOf(dataType),
              TSEncoding.valueOf(encoding),
              TSFileDescriptor.getInstance().getConfig().getCompressor(),
              Collections.emptyMap());
    }
  }

  @After
  public void teardown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDeleteInBufferWriteCache() throws
      StorageEngineException, QueryProcessException, IOException {

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }

    StorageEngine.getInstance().delete(processorName, measurements[3], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 50);
    StorageEngine.getInstance().delete(processorName, measurements[5], 30);
    StorageEngine.getInstance().delete(processorName, measurements[5], 50);

    SingleSeriesExpression expression = new SingleSeriesExpression(new Path(processorName,
        measurements[5]), null);
    QueryDataSource dataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(expression.getSeriesPath(), TEST_QUERY_CONTEXT, null);

    List<ReadOnlyMemChunk> timeValuePairs =
        dataSource.getSeqResources().get(0).getReadOnlyMemChunk();
    int count = 0;
    for (ReadOnlyMemChunk chunk : timeValuePairs) {
      IPointReader iterator = chunk.getPointReader();
      while (iterator.hasNextTimeValuePair()) {
        iterator.nextTimeValuePair();
        count++;
      }
    }
    assertEquals(50, count);
    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void testDeleteInBufferWriteFile()
      throws StorageEngineException, IOException, QueryProcessException {
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    StorageEngine.getInstance().delete(processorName, measurements[5], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 40);
    StorageEngine.getInstance().delete(processorName, measurements[3], 30);

    Modification[] realModifications = new Modification[]{
        new Deletion(new Path(processorName, measurements[5]), 201, 50),
        new Deletion(new Path(processorName, measurements[4]), 202, 40),
        new Deletion(new Path(processorName, measurements[3]), 203, 30),
    };

    File fileNodeDir = new File(DirectoryManager.getInstance().getSequenceFileFolder(0),
        processorName);
    List<File> modFiles = new ArrayList<>();
    for (File directory : fileNodeDir.listFiles()) {
      assertTrue(directory.isDirectory());
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.getPath().endsWith(ModificationFile.FILE_SUFFIX)) {
            modFiles.add(file);
          }
        }
      }
    }

    assertEquals(1, modFiles.size());

    LocalTextModificationAccessor accessor =
        new LocalTextModificationAccessor(modFiles.get(0).getPath());
    try {
      Collection<Modification> modifications = accessor.read();
      assertEquals(3, modifications.size());
      int i = 0;
      for (Modification modification : modifications) {
        TestCase.assertEquals(modification, realModifications[i++]);
      }
    } finally {
      accessor.close();
    }
  }

  @Test
  public void testDeleteInOverflowCache()
      throws StorageEngineException, QueryProcessException, IOException {
    // insert sequence data
    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    // insert unsequence data
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }

    StorageEngine.getInstance().delete(processorName, measurements[3], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 50);
    StorageEngine.getInstance().delete(processorName, measurements[5], 30);
    StorageEngine.getInstance().delete(processorName, measurements[5], 50);

    SingleSeriesExpression expression = new SingleSeriesExpression(new Path(processorName,
        measurements[5]), null);

    QueryDataSource dataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(expression.getSeriesPath(), TEST_QUERY_CONTEXT, null);

    List<ReadOnlyMemChunk> timeValuePairs =
        dataSource.getUnseqResources().get(0).getReadOnlyMemChunk();
    int count = 0;
    for (ReadOnlyMemChunk chunk : timeValuePairs) {
      IPointReader iterator = chunk.getPointReader();
      while (iterator.hasNextTimeValuePair()) {
        iterator.nextTimeValuePair();
        count++;
      }
    }
    assertEquals(50, count);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void testDeleteInOverflowFile()
      throws StorageEngineException, QueryProcessException {
    // insert into BufferWrite
    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    // insert into Overflow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    StorageEngine.getInstance().delete(processorName, measurements[5], 50);
    StorageEngine.getInstance().delete(processorName, measurements[4], 40);
    StorageEngine.getInstance().delete(processorName, measurements[3], 30);

    Modification[] realModifications = new Modification[]{
        new Deletion(new Path(processorName, measurements[5]), 301, 50),
        new Deletion(new Path(processorName, measurements[4]), 302, 40),
        new Deletion(new Path(processorName, measurements[3]), 303, 30),
    };

    File fileNodeDir = new File(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
        processorName);
    List<File> modFiles = new ArrayList<>();
    for (File directory : fileNodeDir.listFiles()) {
      assertTrue(directory.isDirectory());
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.getPath().endsWith(ModificationFile.FILE_SUFFIX)) {
            modFiles.add(file);
          }
        }
      }
    }
    assertEquals(1, modFiles.size());

    LocalTextModificationAccessor accessor =
        new LocalTextModificationAccessor(modFiles.get(0).getPath());
    Collection<Modification> modifications = accessor.read();
    assertEquals(3, modifications.size());
    int i = 0;
    for (Modification modification : modifications) {
      TestCase.assertEquals(modification, realModifications[i++]);
    }
  }
}
