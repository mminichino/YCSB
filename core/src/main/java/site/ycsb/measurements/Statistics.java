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

  private final ConcurrentHashMap<String, AtomicLong> keyStats;
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

  public void updateKey(String name, int bytes) {
    if (keyStats.get(name) == null) {
      AtomicLong dp = new AtomicLong(bytes);
      keyStats.put(name, dp);
    } else {
      keyStats.get(name).addAndGet(bytes);
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
    Histogram keyHistory = new Histogram(Long.MAX_VALUE, 3);

    for (Map.Entry<String, AtomicLong> entry : keyStats.entrySet()) {
      keyHistory.recordValue(entry.getValue().get());
    }

    output.append(String.format("==== Workload Summary %s ====\n", timeStampFormat.format(new Date())));

    output.append(String.format("Reads: %d\n", readOps.get()));
    output.append(String.format("Inserts: %d\n", writeOps.get()));
    output.append(String.format("Updates: %d\n", updateOps.get()));

    long minimum = keyHistory.getMinValue();
    output.append(String.format("Key Min Data Size: %d\n", minimum));
    output.append(String.format("Key Avg Data Size: %.0f\n", keyHistory.getMean()));
    output.append(String.format("Key Max Data Size: %d\n", keyHistory.getMaxValue()));
    long percentile = keyHistory.getValueAtPercentile(50.0);
    long count = round((float) percentile / minimum);
    output.append(String.format("Key 50 percentile: %d (%d) operations\n", percentile, count));
    percentile = keyHistory.getValueAtPercentile(90.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Key 90 percentile: %d (%d) operations\n", percentile, count));
    percentile = keyHistory.getValueAtPercentile(99.0);
    count = round((float) percentile / minimum);
    output.append(String.format("Key 99 percentile: %d (%d) operations\n", percentile, count));

    return output.toString();
  }

}
