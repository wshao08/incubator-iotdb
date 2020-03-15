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
package org.apache.iotdb.db.utils;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This class is used for cleaning test environment in unit test and integration test
 * </p>
 */
public class EnvironmentUtils {

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static DirectoryManager directoryManager = DirectoryManager.getInstance();

  public static long TEST_QUERY_JOB_ID = 1;
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

  private static long oldTsFileThreshold = config.getTsFileSizeThreshold();

  private static int oldMaxMemTableNumber = config.getMaxMemtableNumber();

  private static long oldGroupSizeInByte = config.getMemtableSizeThreshold();

  private static IoTDB daemon;

  public static void cleanEnv() throws IOException, StorageEngineException {
    logger.warn("EnvironmentUtil cleanEnv...");
    if (daemon != null) {
      daemon.stop();
      daemon = null;
    }
    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    TTransport transport = new TSocket("127.0.0.1", 6667, 100);
    if (!transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop daemon failed. 6667 can be connected now.");
        transport.close();
      } catch (TTransportException e) {
      }
    }
    //try sync service
    transport = new TSocket("127.0.0.1", 5555, 100);
    if (!transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop Sync daemon failed. 5555 can be connected now.");
        transport.close();
      } catch (TTransportException e) {
      }
    }
    //try jmx connection
    try {
    JMXServiceURL url =
        new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:31999/jmxrmi");
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
      logger.error("stop JMX failed. 31999 can be connected now.");
    jmxConnector.close();
    } catch (IOException e) {
      //do nothing
    }
    //try MetricService
    try {
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress("127.0.0.1", 8181));
      logger.error("stop MetricService failed. 8181 can be connected now.");
      socket.close();
    } catch (Exception e) {
      //do nothing
    }

    // clean storage group manager
    if (!StorageEngine.getInstance().deleteAll()) {
      logger.error("Can't close the storage group manager in EnvironmentUtils");
      fail();
    }

    IoTDBDescriptor.getInstance().getConfig().setReadOnly(false);


    // clean cache
    if (config.isMetaDataCacheEnable()) {
      TsFileMetaDataCache.getInstance().clear();
      DeviceMetaDataCache.getInstance().clear();
    }
    // close metadata
    MManager.getInstance().clear();

    // delete all directory
    cleanAllDir();

    config.setMaxMemtableNumber(oldMaxMemTableNumber);
    config.setTsFileSizeThreshold(oldTsFileThreshold);
    config.setMemtableSizeThreshold(oldGroupSizeInByte);
    IoTDBConfigDynamicAdapter.getInstance().reset();
  }

  public static void cleanAllDir() throws IOException {
    // delete sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete unsequence files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      cleanDir(path);
    }
    // delete system info
    cleanDir(config.getSystemDir());
    // delete wal
    cleanDir(config.getWalFolder());
    // delete query
    cleanDir(config.getQueryDir());
    cleanDir(config.getBaseDir());
    // delete data files
    for (String dataDir : config.getDataDirs()) {
      cleanDir(dataDir);
    }
  }

  public static void cleanDir(String dir) throws IOException {
    FileUtils.deleteDirectory(new File(dir));
  }

  /**
   * disable the system monitor</br> this function should be called before all code in the setup
   */
  public static void closeStatMonitor() {
    config.setEnableStatMonitor(false);
  }

  /**
   * disable memory control</br> this function should be called before all code in the setup
   */
  public static void envSetUp() {
    logger.warn("EnvironmentUtil setup...");
    System.setProperty(IoTDBConstant.REMOTE_JMX_PORT_NAME, "31999");
    IoTDBDescriptor.getInstance().getConfig().setThriftServerAwaitTimeForStopService(0);
    //we do not start 8181 port in test.
    IoTDBDescriptor.getInstance().getConfig().setEnableMetricService(false);
    if (daemon == null) {
      daemon = new IoTDB();
    }
    try {
      EnvironmentUtils.daemon.active();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    IoTDBDescriptor.getInstance().getConfig().setEnableParameterAdapter(false);
    IoTDBConfigDynamicAdapter.getInstance().setInitialized(true);

    createAllDir();
    // disable the system monitor
    config.setEnableStatMonitor(false);
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId(true);
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  }

  public static void stopDaemon() {
    if(daemon != null) {
      daemon.stop();
    }
  }

  public static void activeDaemon() {
    if(daemon != null) {
      daemon.active();
    }
  }

  public static void reactiveDaemon() {
    if (daemon == null) {
      daemon = new IoTDB();
      daemon.active();
    } else {
      activeDaemon();
    }
  }

  private static void createAllDir() {
    // create sequential files
    for (String path : directoryManager.getAllSequenceFileFolders()) {
      createDir(path);
    }
    // create unsequential files
    for (String path : directoryManager.getAllUnSequenceFileFolders()) {
      createDir(path);
    }
    // create storage group
    createDir(config.getSystemDir());
    // create wal
    createDir(config.getWalFolder());
    // create query
    createDir(config.getQueryDir());
    createDir(TestConstant.OUTPUT_DATA_DIR);
    // create data
    for (String dataDir : config.getDataDirs()) {
      createDir(dataDir);
    }
    //create user and roles folder
    try {
      LocalFileAuthorizer.getInstance().reset();
    } catch (AuthException e) {
      logger.error("create user and role folders failed", e);
      fail(e.getMessage());
    }
  }

  private static void createDir(String dir) {
    File file = new File(dir);
    file.mkdirs();
  }
}
