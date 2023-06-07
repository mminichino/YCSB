/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2020 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.measurements;
import org.HdrHistogram.Histogram;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.round;

/**
 * Collects latency measurements, and reports them when requested.
 */
public class Statistics {

  private static Statistics singleton = null;

  /**
   * Return the singleton Measurements object.
   */
  public static synchronized Statistics getStatistics() {
    if (singleton == null) {
      singleton = new Statistics();
    }
    return singleton;
  }

  private final ConcurrentHashMap<String, DataPoint> keyStats;
  private final AtomicLong readOps;
  private final AtomicLong writeOps;
  private final AtomicLong updateOps;
  /**
   * Operation type.
   */
  public enum OperationType {
    READ,
    INSERT,
    UPDATE,
    APPEND
  }

  public Statistics() {
    keyStats = new ConcurrentHashMap<>();
    readOps = new AtomicLong(0);
    writeOps = new AtomicLong(0);
    updateOps = new AtomicLong(0);
  }

  public void readKey(String name, int data) {
    if (keyStats.get(name) == null) {
      DataPoint dp = new DataPoint();
      dp.update(data, OperationType.READ);
      keyStats.put(name, dp);
    } else {
      keyStats.get(name).update(data, OperationType.READ);
    }
  }

  public void insertKey(String name, int data) {
    if (keyStats.get(name) == null) {
      DataPoint dp = new DataPoint();
      dp.update(data, OperationType.INSERT);
      keyStats.put(name, dp);
    } else {
      keyStats.get(name).update(data, OperationType.INSERT);
    }
  }

  public void updateKey(String name, int data) {
    if (keyStats.get(name) == null) {
      DataPoint dp = new DataPoint();
      dp.update(data, OperationType.UPDATE);
      keyStats.put(name, dp);
    } else {
      keyStats.get(name).update(data, OperationType.UPDATE);
    }
  }

  public void appendKey(String name, int data) {
    if (keyStats.get(name) == null) {
      DataPoint dp = new DataPoint();
      dp.update(data, OperationType.APPEND);
      keyStats.put(name, dp);
    } else {
      keyStats.get(name).update(data, OperationType.APPEND);
    }
  }

  public void incrementRead() {
    readOps.incrementAndGet();
  }

  public void incrementWrite() {
    writeOps.incrementAndGet();
  }

  public void incrementUpdate() {
    updateOps.incrementAndGet();
  }

  /**
   * Return a summary of the statistics.
   */
  public synchronized String getSummary() {
    StringBuilder output = new StringBuilder();
    String dateFormat = "yyyy-MM-dd HH:mm:ss";
    SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
    Histogram read = new Histogram(Long.MAX_VALUE, 3);
    Histogram insert = new Histogram(Long.MAX_VALUE, 3);
    Histogram update = new Histogram(Long.MAX_VALUE, 3);
    Histogram append = new Histogram(Long.MAX_VALUE, 3);

    for (Map.Entry<String, DataPoint> entry : keyStats.entrySet()) {
      DataPoint dp = entry.getValue();
      ConcurrentHashMap<Statistics.OperationType, AtomicLong> dbValues = dp.getValues();
      read.recordValue(dbValues.get(OperationType.READ).get());
      insert.recordValue(dbValues.get(OperationType.INSERT).get());
      update.recordValue(dbValues.get(OperationType.UPDATE).get());
      append.recordValue(dbValues.get(OperationType.APPEND).get());
    }

    output.append(String.format("==== %s ====\n", timeStampFormat.format(new Date())));

    output.append(String.format("Reads: %d\n", readOps.get()));
    output.append(String.format("Inserts: %d\n", writeOps.get()));
    output.append(String.format("Updates: %d\n", updateOps.get()));

    long minimum = read.getMinValue();
    output.append(String.format("Read Min Size: %d\n", minimum));
    output.append(String.format("Read Avg Size: %.0f\n", read.getMean()));
    output.append(String.format("Read Max Size: %d\n", read.getMaxValue()));
    long percentile = read.getValueAtPercentile(50.0);
    long count = round((float) percentile / minimum);
    output.append(String.format("Read 50 percentile: %d (%d) operations\n", percentile, count));
    percentile = read.getValueAtPercentile(90.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Read 90 percentile: %d (%d) operations\n", percentile, count));
    percentile = read.getValueAtPercentile(99.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Read 99 percentile: %d (%d) operations\n", percentile, count));

    minimum = insert.getMinValue();
    output.append(String.format("Insert Min Size: %d\n", minimum));
    output.append(String.format("Insert Avg Size: %.0f\n", insert.getMean()));
    output.append(String.format("Insert Max Size: %d\n", insert.getMaxValue()));
    percentile = insert.getValueAtPercentile(50.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Insert 50 percentile: %d (%d) operations\n", percentile, count));
    percentile = insert.getValueAtPercentile(90.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Insert 90 percentile: %d (%d) operations\n", percentile, count));
    percentile = insert.getValueAtPercentile(99.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Insert 99 percentile: %d (%d) operations\n", percentile, count));

    minimum = update.getMinValue();
    output.append(String.format("Update Min Size: %d\n", minimum));
    output.append(String.format("Update Avg Size: %.0f\n", update.getMean()));
    output.append(String.format("Update Max Size: %d\n", update.getMaxValue()));
    percentile = update.getValueAtPercentile(50.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Update 50 percentile: %d (%d) operations\n", percentile, count));
    percentile = update.getValueAtPercentile(90.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Update 90 percentile: %d (%d) operations\n", percentile, count));
    percentile = update.getValueAtPercentile(99.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Update 99 percentile: %d (%d) operations\n", percentile, count));

    minimum = append.getMinValue();
    output.append(String.format("Append Min Size: %d\n", minimum));
    output.append(String.format("Append Avg Size: %.0f\n", append.getMean()));
    output.append(String.format("Append Max Size: %d\n", append.getMaxValue()));
    percentile = append.getValueAtPercentile(50.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Append 50 percentile: %d (%d) operations\n", percentile, count));
    percentile = append.getValueAtPercentile(90.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Append 90 percentile: %d (%d) operations\n", percentile, count));
    percentile = append.getValueAtPercentile(99.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Append 99 percentile: %d (%d) operations\n", percentile, count));

    return output.toString();
  }

}
