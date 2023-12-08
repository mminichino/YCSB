/*
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
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

package site.ycsb.db.couchbase3;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import static com.couchbase.client.java.kv.MutateInSpec.arrayAppend;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;

import org.slf4j.LoggerFactory;
import site.ycsb.*;
import site.ycsb.measurements.RemoteStatistics;
import site.ycsb.measurements.StatisticsFactory;

/**
 * A class that wraps the 3.x Couchbase SDK to be used with YCSB.
 *
 * <p> The following options can be passed when using this database client to override the defaults.
 *
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>couchbase.bucket=ycsb</b> The bucket name to use.</li>
 * <li><b>couchbase.scope=_default</b> The scope to use.</li>
 * <li><b>couchbase.collection=_default</b> The collection to use.</li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.durability=</b> Durability level to use.</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement.</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement.</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace.</li>
 * <li><b>couchbase.adhoc=false</b> If set to true, prepared statements are not used.</li>
 * <li><b>couchbase.maxParallelism=1</b> The server parallelism for all n1ql queries.</li>
 * <li><b>couchbase.kvEndpoints=1</b> The number of KV sockets to open per server.</li>
 * <li><b>couchbase.sslMode=false</b> Set to true to use SSL to connect to the cluster.</li>
 * <li><b>couchbase.sslNoVerify=true</b> Set to false to check the SSL server certificate.</li>
 * <li><b>couchbase.certificateFile=</b> Path to file containing certificates to trust.</li>
 * <li><b>couchbase.kvTimeout=2000</b> KV operation timeout (milliseconds).</li>
 * <li><b>couchbase.queryTimeout=14000</b> Query timeout (milliseconds).</li>
 * <li><b>couchbase.mode=DEFAULT</b> Test operating mode (DEFAULT or ARRAY).</li>
 * <li><b>couchbase.ttlSeconds=0</b> Set document expiration (TTL) in seconds.</li>
 * </ul>
 */

public class Couchbase3Client extends DB {
  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("com.couchbase.CouchbaseClient");
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  public static final String COUCHBASE_HOST = "couchbase.hostname";
  public static final String COUCHBASE_USER = "couchbase.username";
  public static final String COUCHBASE_PASSWORD = "couchbase.password";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  private static final String KEY_SEPARATOR = "::";
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Cluster cluster;
  private static volatile Collection collection;
  private final ArrayList<Throwable> errors = new ArrayList<>();
  private boolean adhoc;
  private int maxParallelism;
  private static int ttlSeconds;
  private TestType testMode;
  private String arrayKey;
  private boolean loading;
  private boolean collectStats;
  private int clientNumber;
  protected long recordcount;
  private static volatile DurabilityLevel durability = DurabilityLevel.NONE;

  /** Test Type. */
  public enum TestType {
    DEFAULT, ARRAY
  }

  @Override
  public void init() throws DBException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();
    Bucket bucket;
    String couchbasePrefix;

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        properties.load(Files.newInputStream(Paths.get(propFile.getFile())));
      } catch (IOException e) {
        throw new DBException(e);
      }
    }

    properties.putAll(getProperties());

    recordcount =
        Long.parseLong(properties.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }

    String hostname = properties.getProperty(COUCHBASE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String username = properties.getProperty(COUCHBASE_USER, CouchbaseConnect.DEFAULT_USER);
    String password = properties.getProperty(COUCHBASE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    String scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    String collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    boolean sslMode = properties.getProperty("couchbase.sslMode", "false").equals("true");

    durability =
        setDurabilityLevel(Integer.parseInt(properties.getProperty("couchbase.durability", "0")));

    testMode = TestType.valueOf(properties.getProperty("couchbase.mode", "DEFAULT"));
    arrayKey = properties.getProperty("subdoc.arrayKey", "DataArray");

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));

    loading = properties.getProperty("couchbase.loading", "false").equals("true");
    collectStats = properties.getProperty("statistics", "false").equals("true");

    ttlSeconds = Integer.parseInt(properties.getProperty("couchbase.ttlSeconds", "0"));

    if (sslMode) {
      couchbasePrefix = "couchbases://";
    } else {
      couchbasePrefix = "couchbase://";
    }

    String connectString = couchbasePrefix + hostname;

    synchronized (INIT_COORDINATOR) {
      try {
        cluster = Cluster.connect(connectString,
            ClusterOptions.clusterOptions(username, password).environment(env -> {
              env.ioConfig(ioConfig -> {
                ioConfig.numKvConnections(4).networkResolution(NetworkResolution.AUTO)
                    .enableMutationTokens(false);
              }).securityConfig(securityConfig -> {
                securityConfig.enableTls(sslMode)
                    .enableHostnameVerification(false)
                    .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
              }).timeoutConfig(timeoutConfig -> {
                timeoutConfig.kvTimeout(Duration.ofSeconds(2))
                    .connectTimeout(Duration.ofSeconds(5))
                    .queryTimeout(Duration.ofSeconds(75));
              });
            })
        );
      } catch(Exception e) {
        logError(e, connectString);
      }
    }

    try {
      cluster.waitUntilReady(Duration.ofSeconds(5));
    } catch(Exception e) {
      logError(e, connectString);
    }
    bucket = cluster.bucket(bucketName);
    collection = bucket.scope(scopeName).collection(collectionName);

    clientNumber = OPEN_CLIENTS.incrementAndGet();
  }

  private void logError(Exception error, String connectString) {
    Writer buffer = new StringWriter();
    PrintWriter pw = new PrintWriter(buffer);
    error.printStackTrace(pw);
    LOGGER.error(String.format("Connection string: %s", connectString));
    LOGGER.error(pw.toString());
    LOGGER.error(cluster.environment().toString());
    LOGGER.error(cluster.diagnostics().endpoints().toString());
  }

  private DurabilityLevel setDurabilityLevel(final int value) {
    switch(value){
      case 1:
        return DurabilityLevel.MAJORITY;
      case 2:
        return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
      case 3:
        return DurabilityLevel.PERSIST_TO_MAJORITY;
      default :
        return DurabilityLevel.NONE;
    }
  }

  @Override
  public synchronized void cleanup() {
    int runningClients = OPEN_CLIENTS.decrementAndGet();

    if (collectStats && runningClients == 0) {
      RemoteStatistics remoteStatistics = StatisticsFactory.getInstance();
      if (remoteStatistics != null) {
        remoteStatistics.stopCollectionThread();
      }
    }

    for (Throwable t : errors) {
      LOGGER.error(t.getMessage(), t);
    }
  }

  private static <T>T retryBlock(Callable<T> block) throws Exception {
    int retryCount = 10;
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      try {
        return block.call();
      } catch (Exception e) {
        LOGGER.error(String.format("Retry count %d: %s: error: %s", retryCount, e.getClass(), e.getMessage()));
        Writer buffer = new StringWriter();
        PrintWriter pw = new PrintWriter(buffer);
        e.printStackTrace(pw);
        LOGGER.error(String.format("%s", buffer));
        if (retryNumber == retryCount) {
          throw e;
        } else {
          double factor = waitFactor * Math.pow(2, retryNumber);
          long wait = (long) factor;
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    return block.call();
  }

  /**
   * Perform key/value read ("get").
   * @param table The name of the table.
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A HashMap of field/value pairs for the result.
   */
  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      return retryBlock(() -> {
        try {
          collection.get(formatId(table, key), getOptions().transcoder(RawJsonTranscoder.INSTANCE));
          return Status.OK;
        } catch (DocumentNotFoundException e) {
          return Status.NOT_FOUND;
        }
      });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("read failed with exception : " + t);
      return Status.ERROR;
    }
  }

  private static void extractFields(final JsonObject content, Set<String> fields,
                                    final Map<String, ByteIterator> result) {
    if (fields == null || fields.isEmpty()) {
      fields = content.getNames();
    }

    for (String field : fields) {
      result.put(field, new StringByteIterator(content.getString(field)));
    }
  }

  public void upsertArray(String id, String arrayKey, Object content) throws Exception {
    retryBlock(() -> {
      try {
        collection.mutateIn(id, Collections.singletonList(arrayAppend(arrayKey, Collections.singletonList(content))),
            mutateInOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
      } catch (DocumentNotFoundException e) {
        com.google.gson.JsonObject document = new com.google.gson.JsonObject();
        com.google.gson.JsonArray subDocArray = new com.google.gson.JsonArray();
        Gson gson = new Gson();
        String subDoc = gson.toJson(content);
        subDocArray.add(gson.fromJson(subDoc, com.google.gson.JsonObject.class));
        document.add(arrayKey, subDocArray);
        collection.upsert(id, document, upsertOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
      }
      return null;
    });
  }

  public void insertArray(String id, String arrayKey, Object content) throws Exception {
    retryBlock(() -> {
      com.google.gson.JsonObject document = new com.google.gson.JsonObject();
      com.google.gson.JsonArray subDocArray = new com.google.gson.JsonArray();
      Gson gson = new Gson();
      String subDoc = gson.toJson(content);
      subDocArray.add(gson.fromJson(subDoc, com.google.gson.JsonObject.class));
      document.add(arrayKey, subDocArray);
      collection.upsert(id, document, upsertOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
      return null;
    });
  }

  /**
   * Update record.
   * @param table The name of the table.
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record.
   */
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      return retryBlock(() -> {
        if (Objects.requireNonNull(testMode) == TestType.ARRAY) {
          upsertArray(formatId(table, key), arrayKey, encode(values));
        } else {
          collection.upsert(formatId(table, key), encode(values),
              upsertOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
        }
        return Status.OK;
      });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  /**
   * Insert a record.
   * @param table The name of the table.
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record.
   */
  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      return retryBlock(() -> {
        if (Objects.requireNonNull(testMode) == TestType.ARRAY) {
          insertArray(formatId(table, key), arrayKey, encode(values));
        } else {
          collection.upsert(formatId(table, key), encode(values),
              upsertOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
        }
        return Status.OK;
      });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  /**
   * Helper method to turn the passed in iterator values into a map we can encode to json.
   *
   * @param values the values to encode.
   * @return the map of encoded values.
   */
  private static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      result.put(value.getKey(), value.getValue().toString());
    }
    return result;
  }

  /**
   * Remove a record.
   * @param table The name of the table.
   * @param key The record key of the record to delete.
   */
  @Override
  public Status delete(final String table, final String key) {
    try {
      return retryBlock(() -> {
        collection.remove(formatId(table, key));
        return Status.OK;
      });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("delete failed with exception :" + t);
      return Status.ERROR;
    }
  }

  /**
   * Query for specific rows of data using SQL++.
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   */
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      retryBlock(() -> {
        ReactiveCluster reactor = cluster.reactive();
        long offset = getKeyHashNum(startkey);
        final String query = String.format("select * from ycsb offset %d limit %d;", offset, recordcount);
        final List<HashMap<String, ByteIterator>> data = new ArrayList<>();
        reactor.query(query, queryOptions().maxParallelism(maxParallelism).adhoc(adhoc))
            .flatMapMany(res -> res.rowsAs(String.class))
            .doOnError(e -> {
              throw new RuntimeException(e.getMessage());
            })
            .onErrorStop()
            .map(getResult -> {
              HashMap<String, ByteIterator> tuple = new HashMap<>();
              decodeStringSource(getResult, fields, tuple);
              return tuple;
            })
            .toStream()
            .forEach(data::add);
        result.addAll(data);
        return null;
      });
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("scan failed with exception :" + t);
      return Status.ERROR;
    }
  }

  /**
   * Get string values from fields.
   * @param source JSON source data.
   * @param fields Fields to return.
   * @param dest Map of Strings where each value is a requested field.
   */
  private void decodeStringSource(final String source, final Set<String> fields,
                      final Map<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && !fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.asText()));
        }
      }
    } catch (Exception e) {
      LOGGER.error("Could not decode JSON response from scanSpecificFields");
    }
  }

  /**
   * Helper method to turn the prefix and key into a proper document ID.
   *
   * @param prefix the prefix (table).
   * @param key the key itself.
   * @return a document ID that can be used with Couchbase.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + KEY_SEPARATOR + key;
  }

  private long getKeyHashNum(String key) {
    return Math.abs(key.hashCode()) % recordcount;
  }
}
