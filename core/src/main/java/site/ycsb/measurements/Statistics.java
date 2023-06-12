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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;

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

  private final ConcurrentHashMap<String, AtomicInteger> keyStats;

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
  }

  public void updateKey(String name) {
    if (keyStats.get(name) == null) {
      AtomicInteger dp = new AtomicInteger(1);
      keyStats.put(name, dp);
    } else {
      keyStats.get(name).incrementAndGet();
    }
  }

  public static HashMap<String, Integer> sortMap(ConcurrentHashMap<String, AtomicInteger> input)
  {
    List<Map.Entry<String, AtomicInteger>> list = new LinkedList<>(input.entrySet());

    list.sort((o1, o2) -> Integer.compare(o2.getValue().get(), o1.getValue().get()));

    HashMap<String, Integer> temp = new LinkedHashMap<>();
    for (Map.Entry<String, AtomicInteger> item : list) {
      temp.put(item.getKey(), item.getValue().get());
    }
    return temp;
  }

  /**
   * Return a summary of the statistics.
   */
  public synchronized String getSummary() {
    StringBuilder output = new StringBuilder();
    HashMap<String, Integer> sortedData = sortMap(keyStats);
    int number = 1;

    output.append("Number,Key,Count\n");

    for (Map.Entry<String, Integer> entry : sortedData.entrySet()) {
      output.append(String.format("%d,%s,%d\n", number++, entry.getKey(), entry.getValue()));
    }

    return output.toString();
  }

}
