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
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * Statistics for string type.
 */
public class BinaryStatistics extends Statistics<Binary> {

  private Binary firstValue = new Binary("");
  private Binary lastValue = new Binary("");

  @Override
  public TSDataType getType() {
    return TSDataType.TEXT;
  }

  @Override
  public int getStatsSize() {
    return 4 + firstValue.getValues().length
        + 4 + lastValue.getValues().length;
  }

  /**
   * initialize Statistics.
   *
   * @param first the first value
   * @param last the last value
   */
  private void initializeStats(Binary first, Binary last) {
    this.firstValue = first;
    this.lastValue = last;
  }

  private void updateStats(Binary firstValue, Binary lastValue) {
    this.lastValue = lastValue;
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
  }

  @Override
  public Binary getMinValue() {
    throw new StatisticsClassException("Binary statistics does not support: min");
  }

  @Override
  public Binary getMaxValue() {
    throw new StatisticsClassException("Binary statistics does not support: max");
  }

  @Override
  public Binary getFirstValue() {
    return firstValue;
  }

  @Override
  public Binary getLastValue() {
    return lastValue;
  }

  @Override
  public double getSumValue() {
    throw new StatisticsClassException("Binary statistics does not support: sum");
  }

  @Override
  protected void mergeStatisticsValue(Statistics stats) {
    BinaryStatistics stringStats = (BinaryStatistics) stats;
    if (isEmpty) {
      initializeStats(stringStats.getFirstValue(), stringStats.getLastValue());
      isEmpty = false;
    } else {
      updateStats(stringStats.getFirstValue(), stringStats.getLastValue());
    }
  }

  @Override
  void updateStats(Binary value) {
    if (isEmpty) {
      initializeStats(value, value);
      isEmpty = false;
    } else {
      updateStats(value, value);
    }
  }

  @Override
  void updateStats(Binary[] values, int batchSize) {
    for (int i = 0; i < batchSize; i++) {
      updateStats(values[i]);
    }
  }

  @Override
  public byte[] getMinValueBytes() {
    throw new StatisticsClassException("Binary statistics does not support: min");
  }

  @Override
  public byte[] getMaxValueBytes() {
    throw new StatisticsClassException("Binary statistics does not support: max");
  }

  @Override
  public byte[] getFirstValueBytes() {
    return firstValue.getValues();
  }

  @Override
  public byte[] getLastValueBytes() {
    return lastValue.getValues();
  }

  @Override
  public byte[] getSumValueBytes() {
    throw new StatisticsClassException("Binary statistics does not support: sum");
  }

  @Override
  public ByteBuffer getMinValueBuffer() {
    throw new StatisticsClassException("Binary statistics does not support: min");
  }

  @Override
  public ByteBuffer getMaxValueBuffer() {
    throw new StatisticsClassException("Binary statistics does not support: max");
  }

  @Override
  public ByteBuffer getFirstValueBuffer() {
    return ByteBuffer.wrap(firstValue.getValues());
  }

  @Override
  public ByteBuffer getLastValueBuffer() {
    return ByteBuffer.wrap(lastValue.getValues());
  }

  @Override
  public ByteBuffer getSumValueBuffer() {
    throw new StatisticsClassException("Binary statistics does not support: sum");
  }

  @Override
  public int serializeStats(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(firstValue, outputStream);
    byteLen += ReadWriteIOUtils.write(lastValue, outputStream);
    return byteLen;
  }

  @Override
  void deserialize(InputStream inputStream) throws IOException {
    this.firstValue = ReadWriteIOUtils.readBinary(inputStream);
    this.lastValue = ReadWriteIOUtils.readBinary(inputStream);
  }

  @Override
  void deserialize(ByteBuffer byteBuffer) {
    this.firstValue = ReadWriteIOUtils.readBinary(byteBuffer);
    this.lastValue = ReadWriteIOUtils.readBinary(byteBuffer);
  }

  @Override
  public String toString() {
    return super.toString() + " [fistValue:" + firstValue + ",lastValue:" + lastValue + "]";
  }

}
