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
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import site.ycsb.*;

import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Aerospike</a>.
 */
public class AerospikeClient extends DB {
  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.aerospike.AerospikeClient");
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
    } catch (Throwable ex) {
      LOGGER.error("read error: {}", ex.getMessage(), ex);
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
      Statement stmt = new Statement();
      stmt.setNamespace(namespace);
      stmt.setSetName(table);

      QueryPolicy policy = new QueryPolicy();
      policy.setMaxRecords(count);
      policy.filterExp = Exp.build(
          Exp.ge(Exp.key(Exp.Type.STRING), Exp.val(start))
      );

      RecordSet rs = client.query(policy, stmt);

      while (rs.next()) {
        Record record = rs.getRecord();
        result.add(extractResult(record.bins));
      }
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("scan error: {}", ex.getMessage(), ex);
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
