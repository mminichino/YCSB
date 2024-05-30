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

import ch.qos.logback.classic.Logger;
import com.aerospike.client.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import site.ycsb.*;

import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Aerospike</a>.
 */
public class AerospikeClient extends DB {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.aerospike.AerospikeClient");
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";

  private String namespace = null;
  private int timeout = 10000;

  private com.aerospike.client.AerospikeClient client = null;

  private final Policy readPolicy = new Policy();
  private final WritePolicy insertPolicy = new WritePolicy();
  private final WritePolicy updatePolicy = new WritePolicy();
  private final WritePolicy deletePolicy = new WritePolicy();

  private static final AtomicInteger recordId = new AtomicInteger(0);

  @Override
  public void init() throws DBException {
    insertPolicy.recordExistsAction = RecordExistsAction.UPDATE;
    updatePolicy.recordExistsAction = RecordExistsAction.UPDATE;

    Properties props = getProperties();

    namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);

    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    timeout = Integer.parseInt(props.getProperty("as.timeout", DEFAULT_TIMEOUT));
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
    } catch (Throwable ex) {
      LOGGER.error("read error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }

  private HashMap<String, ByteIterator> extractResult(Set<Map.Entry<String, Object>> values) {
    HashMap<String, ByteIterator> result = new HashMap<>();
    values.parallelStream()
        .forEach(entry -> result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()))
//        .forEach(entry -> LOGGER.info("{} : {}", entry.getKey(), entry.getValue().toString())
        );
    return result;
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      Record record = client.get(readPolicy, new Key(namespace, table, start), "id");
      int startId = record.getInt("id");

      Statement stmt = new Statement();
      stmt.setNamespace(namespace);
      stmt.setSetName(table);

      QueryPolicy policy = new QueryPolicy();
      policy.includeBinData = false;
      policy.totalTimeout = timeout;
      policy.socketTimeout = timeout;
      policy.connectTimeout = timeout;
      policy.setMaxRecords(count);
      policy.filterExp = Exp.build(
          Exp.ge(Exp.intBin("id"), Exp.val(startId))
      );

      RecordSet rs = client.query(policy, stmt);

      while (rs.next()) {
        Record document = client.get(readPolicy, rs.getKey());
        result.add(extractResult(document.bins.entrySet()));
      }
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("scan error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }

  private Status write(String table, String key, WritePolicy writePolicy, Map<String, ByteIterator> values) {
    List<Bin> bins = new ArrayList<>();
    Bin[] binList = new Bin[0];

    bins.add(new Bin("id", recordId.incrementAndGet()));

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      bins.add(new Bin(entry.getKey(), entry.getValue().toString()));
    }

    Key keyObj = new Key(namespace, table, key);

    try {
      client.put(writePolicy, keyObj, bins.toArray(binList));
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("write error: {}", ex.getMessage(), ex);
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
        LOGGER.error("key not found for delete {}", key);
        return Status.ERROR;
      }

      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("delete error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }
}
