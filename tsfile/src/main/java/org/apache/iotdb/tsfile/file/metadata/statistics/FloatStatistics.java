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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Statistics for float type.
 */
public class FloatStatistics extends Statistics<Float> {

  private float minValue;
  private float maxValue;
  private float firstValue;
  private float lastValue;
  private double sumValue;

  @Override
  public TSDataType getType() {
    return TSDataType.FLOAT;
  }

  @Override
  public int getStatsSize() {
    return 24;
  }

  private void initializeStats(float min, float max, float first, float last, double sum) {
    this.minValue = min;
    this.maxValue = max;
    this.firstValue = first;
    this.lastValue = last;
    this.sumValue = sum;
  }

  private void updateStats(float minValue, float maxValue, float firstValue, float last,
      double sumValue) {
    if (minValue < this.minValue) {
      this.minValue = minValue;
    }
    if (maxValue > this.maxValue) {
      this.maxValue = maxValue;
    }
    this.sumValue += sumValue;
    this.lastValue = last;
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    minValue = BytesUtils.bytesToFloat(minBytes);
    maxValue = BytesUtils.bytesToFloat(maxBytes);
  }

  @Override
  void updateStats(float value) {
    if (this.isEmpty) {
      initializeStats(value, value, value, value, value);
      isEmpty = false;
    } else {
      updateStats(value, value, value, value, value);
    }
  }

  @Override
  void updateStats(float[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public Float getMinValue() {
    return minValue;
  }

  @Override
  public Float getMaxValue() {
    return maxValue;
  }

  @Override
  public Float getFirstValue() {
    return firstValue;
  }

  @Override
  public Float getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumValue() {
    return sumValue;
  }

  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    FloatStatistics floatStats = (FloatStatistics) stats;
    if (isEmpty) {
      initializeStats(floatStats.getMinValue(), floatStats.getMaxValue(), floatStats.getFirstValue(),
          floatStats.getLastValue(), floatStats.getSumValue());
      isEmpty = false;
    } else {
      updateStats(floatStats.getMinValue(), floatStats.getMaxValue(), floatStats.getFirstValue(),
          floatStats.getLastValue(), floatStats.getSumValue());
    }
  }

  @Override
  public byte[] getMinValueBytes() {
    return BytesUtils.floatToBytes(minValue);
  }

  @Override
  public byte[] getMaxValueBytes() {
    return BytesUtils.floatToBytes(maxValue);
  }

  @Override
  public byte[] getFirstValueBytes() {
    return BytesUtils.floatToBytes(firstValue);
  }

  @Override
  public byte[] getLastValueBytes() {
    return BytesUtils.floatToBytes(lastValue);
  }

  @Override
  public byte[] getSumValueBytes() {
    return BytesUtils.doubleToBytes(sumValue);
  }

  @Override
  public ByteBuffer getMinValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(minValue);
  }

  @Override
  public ByteBuffer getMaxValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(maxValue);
  }

  @Override
  public ByteBuffer getFirstValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(firstValue);
  }

  @Override
  public ByteBuffer getLastValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(lastValue);
  }

  @Override
  public ByteBuffer getSumValueBuffer() {
    return ReadWriteIOUtils.getByteBuffer(sumValue);
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(minValue, outputStream);
    byteLen += ReadWriteIOUtils.write(maxValue, outputStream);
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    byteLen += ReadWriteIOUtils.write(sumValue, outputStream);
    return byteLen;
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.minValue = ReadWriteIOUtils.readFloat(inputStream);
    this.maxValue = ReadWriteIOUtils.readFloat(inputStream);
    this.firstValue = ReadWriteIOUtils.readFloat(inputStream);
    this.lastValue = ReadWriteIOUtils.readFloat(inputStream);
    this.sumValue = ReadWriteIOUtils.readDouble(inputStream);
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) {
    this.minValue = ReadWriteIOUtils.readFloat(byteBuffer);
    this.maxValue = ReadWriteIOUtils.readFloat(byteBuffer);
    this.firstValue = ReadWriteIOUtils.readFloat(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readFloat(byteBuffer);
    this.sumValue = ReadWriteIOUtils.readDouble(byteBuffer);
  }

  @Override
  public String toString() {
    return super.toString() + " [minValue:" + minValue + ",maxValue:" + maxValue + ",firstValue:" + firstValue +
        ",lastValue:" + lastValue + ",sumValue:" + sumValue + "]";
  }
}
