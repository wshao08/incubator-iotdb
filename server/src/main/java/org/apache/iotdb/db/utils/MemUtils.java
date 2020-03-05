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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Notice : methods in this class may not be accurate.
public class MemUtils {

  private static Logger logger = LoggerFactory.getLogger(MemUtils.class);

  private MemUtils(){

  }

  /**
   * function for getting the record size.
   */
  public static long getRecordSize(InsertPlan insertPlan) {
    long memSize = 0;
    for (int i = 0; i < insertPlan.getValues().length; i++) {
      switch (insertPlan.getDataTypes()[i]) {
        case INT32:
          memSize += 8L + 4L; break;
        case INT64:
          memSize += 8L + 8L; break;
        case FLOAT:
          memSize += 8L + 4L; break;
        case DOUBLE:
          memSize += 8L + 8L; break;
        case BOOLEAN:
          memSize += 8L + 1L; break;
        case TEXT:
          memSize += 8L + insertPlan.getValues()[i].length(); break;
        default:
          memSize += 8L + 8L;
      }
    }
    return memSize;
  }

  public static long getRecordSize(BatchInsertPlan batchInsertPlan, int start, int end) {
    if (start >= end) {
      return 0L;
    }
    long memSize = 0;
    for (int i = 0; i < batchInsertPlan.getMeasurements().length; i++) {
      switch (batchInsertPlan.getDataTypes()[i]) {
        case INT32:
          memSize += (end - start) * (8L + 4L); break;
        case INT64:
          memSize += (end - start) * (8L + 8L); break;
        case FLOAT:
          memSize += (end - start) * (8L + 4L); break;
        case DOUBLE:
          memSize += (end - start) * (8L + 8L); break;
        case BOOLEAN:
          memSize += (end - start) * (8L + 1L); break;
        case TEXT:
          memSize += (end - start) * 8L;
          for (int j = start; j < end; j++) {
            memSize += ((Binary[]) batchInsertPlan.getColumns()[i])[j].getLength();
          }
          break;
        default:
          memSize += (end - start) * (8L + 8L);
      }
    }
    return memSize;
  }

  /**
   * Calculate how much memory will be used if the given record is written to sequence file.
   */
  public static long getTsRecordMem(TSRecord record) {
    long memUsed = 8; // time
    memUsed += 8; // deviceId reference
    memUsed += getStringMem(record.deviceId);
    for (DataPoint dataPoint : record.dataPointList) {
      memUsed += 8; // dataPoint reference
      memUsed += getDataPointMem(dataPoint);
    }
    return memUsed;
  }

  /**
   * function for getting the memory size of the given string.
   */
  public static long getStringMem(String str) {
    // wide char (2 bytes each) and 64B String overhead
    return str.length() * 2 + 64L;
  }

  /**
   * function for getting the memory size of the given data point.
   */
  public static long getDataPointMem(DataPoint dataPoint) {
    // type reference
    long memUsed = 8;
    // measurementId and its reference
    memUsed += getStringMem(dataPoint.getMeasurementId());
    memUsed += 8;

    if (dataPoint instanceof FloatDataPoint) {
      memUsed += 4;
    } else if (dataPoint instanceof IntDataPoint) {
      memUsed += 4;
    } else if (dataPoint instanceof BooleanDataPoint) {
      memUsed += 1;
    } else if (dataPoint instanceof DoubleDataPoint) {
      memUsed += 8;
    } else if (dataPoint instanceof LongDataPoint) {
      memUsed += 8;
    } else if (dataPoint instanceof StringDataPoint) {
      StringDataPoint stringDataPoint = (StringDataPoint) dataPoint;
      memUsed += 8 + 20; // array reference and array overhead
      memUsed += ((Binary) stringDataPoint.getValue()).getLength();
      // encoding string reference and its memory
      memUsed += 8;
      memUsed += getStringMem(((Binary) stringDataPoint.getValue()).getTextEncodingType());
    } else {
      logger.error("Unsupported data point type");
    }

    return memUsed;
  }

  /**
   * function for converting the byte count result to readable string.
   */
  public static String bytesCntToStr(long inputCnt) {
    long cnt = inputCnt;
    long gbs = cnt / IoTDBConstant.GB;
    cnt = cnt % IoTDBConstant.GB;
    long mbs = cnt / IoTDBConstant.MB;
    cnt = cnt % IoTDBConstant.MB;
    long kbs = cnt / IoTDBConstant.KB;
    cnt = cnt % IoTDBConstant.KB;
    return gbs + " GB " + mbs + " MB " + kbs + " KB " + cnt + " B";
  }
}
