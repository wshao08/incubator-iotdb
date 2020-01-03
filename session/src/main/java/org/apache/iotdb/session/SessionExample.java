package org.apache.iotdb.session;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionExample {

  private static Session session;
  private static final Logger LOG = LoggerFactory.getLogger(SessionExample.class);
  private static int sgNum = 1;

  public static void main(String[] args)
      throws IoTDBSessionException {
    if (args.length == 0) {
      LOG.info("Please input IP");
      System.exit(1);
    }
    session = new Session(args[0], 6667, "root", "root");
    session.open();

    //nohup java -Xms500m -Xmx500m -Xmn250m -Xss256k -server -XX:+HeapDumpOnOutOfMemoryError -jar client-example.jar &

    int operFlag = 1;
    try {
      operFlag = Integer.parseInt(args[0]);
    } catch (Exception e) {

    }

     /*final int beginTag=1;
      final long beginTime=1514451315000L;//2017.12.28
      final long endTime=System.currentTimeMillis();//1530720000000L;*/

    ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(sgNum);
    if (operFlag == 1) {

      for (int i = 1; i <= sgNum; i++) {
        final int finalI = i;
        exec.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            try {
              insert(String.format("root.group%d", finalI), 0);
            } catch (Exception e) {
              e.printStackTrace();
              LOG.error("-----------------{}----------------", e.getMessage(), e);
            }
          }
        }, 0, 5000, TimeUnit.MILLISECONDS);
      }

    }

    //query();



    /*insert();
    insertInBatch();
    insertRowBatch();
    nonQuery();
    query();
    deleteData();
    deleteTimeseries();*/

//    session.close();
  }


  private static void insert(String deviceId, int beginTag) throws IoTDBSessionException {
    List<String> measurements = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (int i = beginTag * 100000 + 1; i < (beginTag + 1) * 100000 + 1; i++) {
      measurements.add(String.format("s%d", i));
      values.add(Math.random() + "");
    }
    long begin = System.nanoTime();
    session.insert(deviceId, System.currentTimeMillis(), measurements, values);
    LOG.info("Device(SG){}写数据花费：{}ms", deviceId, (System.nanoTime() - begin) / 1_000_000);
  }

  private static void insert() throws IoTDBSessionException {
    String deviceId = "root.group.d1";
    List<String> measurements = new ArrayList<>();

    for (int i = 1; i < 50001; i++) {
      measurements.add(String.format("s%d", i));
    }
    for (int i = 1; i < 10; i++) {//10row
      List<String> values = new ArrayList<>();
      for (int j = 1; j < 50001; j++) {//5w points
        values.add(Math.random() + "");
      }
      long begin = System.nanoTime();
      session.insert(deviceId, System.currentTimeMillis(), measurements, values);
      System.out.println((System.nanoTime() - begin) / 1000000000 + "s");
    }
  }

  private static void insertInBatch() throws IoTDBSessionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<String>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertInBatch(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertInBatch(deviceIds, timestamps, measurementsList, valuesList);
  }

  private static void insertRowBatch() throws IoTDBSessionException {
    Schema schema = new Schema();
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch = schema.createRowBatch("root.sg1.d1", 100);

    long[] timestamps = rowBatch.timestamps;
    Object[] values = rowBatch.values;

    for (long time = 0; time < 100; time++) {
      int row = rowBatch.batchSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (rowBatch.batchSize == rowBatch.getMaxBatchSize()) {
        session.insertBatch(rowBatch);
        rowBatch.reset();
      }
    }

    if (rowBatch.batchSize != 0) {
      session.insertBatch(rowBatch);
      rowBatch.reset();
    }
  }

  private static void deleteData() throws IoTDBSessionException {
    String path = "root.sg1.d1.s1";
    long deleteTime = 99;
    session.deleteData(path, deleteTime);
  }

  private static void deleteTimeseries() throws IoTDBSessionException {
    List<String> paths = new ArrayList<>();
    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d1.s2");
    paths.add("root.sg1.d1.s3");
    session.deleteTimeseries(paths);
  }

  private static void query() throws TException, IoTDBRPCException, SQLException {
    //SessionDataSet dataSet = session.executeQueryStatement("select * from root.dtxy.d1");
    StringBuffer sb = new StringBuffer();
    sb.append("select ");
    for (int i = 1; i < 101; i++) {
      sb.append("last_value(s").append(i).append("),max_time(s").append(i).append("),");
    }
    long begin = System.nanoTime();
    SessionDataSet dataSet = session
        .executeQueryStatement(sb.substring(0, sb.length() - 1) + " from root.dtxy.group1");
    System.out.println((System.nanoTime() - begin) / 1000000000 + "s");
    //SessionDataSet dataSet = session.executeQueryStatement("select last_value(s1),max_time(s1) from root.dtxy.d1");
    dataSet.setBatchSize(1024); // default is 512
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void nonQuery() throws TException, IoTDBRPCException, SQLException {
    session.executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1);");
  }
}