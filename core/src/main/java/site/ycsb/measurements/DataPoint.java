/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single statistic.
 */
public class DataPoint {

  private final AtomicLong insert;
  private final AtomicLong update;
  private final AtomicLong read;
  private final AtomicLong append;

  public DataPoint() {
    insert = new AtomicLong(0);
    update = new AtomicLong(0);
    read = new AtomicLong(0);
    append = new AtomicLong(0);
  }

  public void update(int data, Statistics.OperationType type) {
    switch (type) {
    case READ:
      read.addAndGet(data);
      break;
    case INSERT:
      insert.addAndGet(data);
      break;
    case UPDATE:
      update.addAndGet(data);
      break;
    case APPEND:
      append.addAndGet(data);
      break;
    default:
      throw new IllegalArgumentException("unknown operation type argument");
    }
  }

  public ConcurrentHashMap<Statistics.OperationType, AtomicLong> getValues() {
    ConcurrentHashMap<Statistics.OperationType, AtomicLong> values = new ConcurrentHashMap<>();
    values.put(Statistics.OperationType.READ, read);
    values.put(Statistics.OperationType.INSERT, insert);
    values.put(Statistics.OperationType.UPDATE, update);
    values.put(Statistics.OperationType.APPEND, append);
    return values;
  }

}
