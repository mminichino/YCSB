/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 * -
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * -
 * http://www.apache.org/licenses/LICENSE-2.0
 * -
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.*;
import site.ycsb.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Aerospike</a>.
 */
public class AerospikeClient extends DB {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";

  private String namespace = null;

  private com.aerospike.client.AerospikeClient client = null;

  private final Policy readPolicy = new Policy();
  private final WritePolicy insertPolicy = new WritePolicy();
  private final WritePolicy updatePolicy = new WritePolicy();
  private final WritePolicy deletePolicy = new WritePolicy();
  private final ScanPolicy scanPolicy = new ScanPolicy();

  @Override
  public void init() throws DBException {
    insertPolicy.recordExistsAction = RecordExistsAction.UPDATE;
    updatePolicy.recordExistsAction = RecordExistsAction.UPDATE;
    scanPolicy.includeBinData = false;

    Properties props = getProperties();

    namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);

    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    int timeout = Integer.parseInt(props.getProperty("as.timeout",
        DEFAULT_TIMEOUT));
    boolean external = getProperties().getProperty("as.external", "false").equals("true");

    ClientPolicy clientPolicy = new ClientPolicy();

    if (user != null && password != null) {
      clientPolicy.user = user;
      clientPolicy.password = password;
      clientPolicy.timeout = timeout;
    }

    if (external) {
      clientPolicy.useServicesAlternate = true;
    }

    try {
      client =
          new com.aerospike.client.AerospikeClient(clientPolicy, host, port);
    } catch (AerospikeException e) {
      throw new DBException(String.format("Error while creating Aerospike " +
          "client for %s:%d.", host, port), e);
    }
  }

  @Override
  public void cleanup() {
    client.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Record record;
      int size = (fields == null) ? 0 : fields.size();
      String[] fieldList = new String[size];

      if (fields != null) {
        record = client.get(readPolicy, new Key(namespace, table, key),
            fields.toArray(fieldList));
      } else {
        record = client.get(readPolicy, new Key(namespace, table, key));
      }

      if (record == null) {
        System.err.println("Record key " + key + " not found (read)");
        return Status.ERROR;
      }

      for (Map.Entry<String, Object> entry: record.bins.entrySet()) {
        result.put(entry.getKey(),
            new StringByteIterator(entry.getValue().toString()));
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while reading key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  private HashMap<String, ByteIterator> extractResult(Map<String, Object> values) {
    HashMap<String, ByteIterator> result = new HashMap<>();
    values.entrySet()
        .parallelStream()
        .forEach(entry -> result.put(entry.getKey(), new StringByteIterator((String) entry.getValue())));
    return result;
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      AtomicInteger recordCount = new AtomicInteger(1);
      client.scanAll(scanPolicy, namespace, table, (key, record) -> {
        if (key.toString().compareTo(start) <= 0 && recordCount.get() <= count) {
          Record document = client.get(readPolicy, key);
          result.add(extractResult(document.bins));
          recordCount.incrementAndGet();
        }
      });
      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while scanning from key " + start + ": " + e);
      return Status.ERROR;
    }
  }

  private Status write(String table, String key, WritePolicy writePolicy, Map<String, ByteIterator> values) {
    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      bins[index] = new Bin(entry.getKey(), entry.getValue().toString());
      ++index;
    }

    Key keyObj = new Key(namespace, table, key);

    try {
      client.put(writePolicy, keyObj, bins);
      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while writing key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return write(table, key, updatePolicy, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return write(table, key, insertPolicy, values);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (!client.delete(deletePolicy, new Key(namespace, table, key))) {
        System.err.println("Record key " + key + " not found (delete)");
        return Status.ERROR;
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return Status.ERROR;
    }
  }
}
