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
//import com.couchbase.client.core.env.IoConfig;
//import com.couchbase.client.core.env.SecurityConfig;
//import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
//import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.query.ReactiveQueryResult;
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
import reactor.core.publisher.Mono;

import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.RemoteStatistics;
import site.ycsb.measurements.Statistics;
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
  protected static final ch.qos.logback.classic.Logger STATISTICS =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("com.couchbase.statistics");
  protected static final ch.qos.logback.classic.Logger KEY_STATS =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("com.couchbase.KeyStats");
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  public static final String COUCHBASE_HOST = "couchbase.hostname";
  public static final String COUCHBASE_USER = "couchbase.username";
  public static final String COUCHBASE_PASSWORD = "couchbase.password";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  public static final String COUCHBASE_PROJECT = "couchbase.project";
  public static final String COUCHBASE_DATABASE = "couchbase.database";
  public static CouchbaseConnect db;
  private static final String KEY_SEPARATOR = "::";
  private static final String KEYSPACE_SEPARATOR = ".";
  private static volatile ClusterEnvironment environment;
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Cluster cluster;
  private static volatile ReactiveCluster reactiveCluster;
  private static volatile Bucket bucket;
  private static volatile Collection collection;
  private static volatile ClusterOptions clusterOptions;
  private volatile PersistTo persistTo;
  private volatile ReplicateTo replicateTo;
  private volatile boolean useDurabilityLevels;
  private final ArrayList<Throwable> errors = new ArrayList<>();
  private boolean adhoc;
  private int maxParallelism;
  private String bucketName;
  private String scopeName;
  private String collectionName;
  private String capellaProject;
  private String capellaDatabase;
  private static boolean collectionEnabled;
  private static boolean scopeEnabled;
  private static String keyspaceName;
  private static int ttlSeconds;
  private static int durabilityLevel;
  private static volatile AtomicInteger primaryKeySeq;
  private TestType testMode;
  private String arrayKey;
  private RemoveOptions dbRemoveOptions;
  private MutateInOptions dbMutateOptions;
  private InsertOptions dbInsertOptions;
  private ReplaceOptions dbReplaceOptions;
  private UpsertOptions dbUpsertOptions;
  private boolean doUpsert;
  private boolean loading;
  private boolean collectStats;
  private boolean collectKeyStats;
  private final Statistics statistics = Statistics.getStatistics();
  private int clientNumber;
  private final String recordId = "record_id";

  /** Test Type. */
  public enum TestType {
    DEFAULT, ARRAY
  }

  @Override
  public void init() throws DBException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();
    primaryKeySeq = new AtomicInteger();
    CouchbaseConnect.CouchbaseBuilder builder = new CouchbaseConnect.CouchbaseBuilder();

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      System.err.println("Using file");
      try {
        properties.load(Files.newInputStream(Paths.get(propFile.getFile())));
      } catch (IOException e) {
        throw new DBException(e);
      }
    } else {
      properties = getProperties();
    }

    String hostname = properties.getProperty(COUCHBASE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String username = properties.getProperty(COUCHBASE_USER, CouchbaseConnect.DEFAULT_USER);
    String password = properties.getProperty(COUCHBASE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");

    capellaProject = properties.getProperty(COUCHBASE_PROJECT, null);
    capellaDatabase = properties.getProperty(COUCHBASE_DATABASE, null);

    durabilityLevel = Integer.parseInt(properties.getProperty("couchbase.durability", "0"));

    testMode = TestType.valueOf(properties.getProperty("couchbase.mode", "DEFAULT"));
    arrayKey = properties.getProperty("subdoc.arrayKey", "DataArray");

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));

    doUpsert = properties.getProperty("couchbase.upsert", "false").equals("true");
    loading = properties.getProperty("couchbase.loading", "false").equals("true");
    collectStats = properties.getProperty("statistics", "false").equals("true");
    collectKeyStats = properties.getProperty("keyStatistics", "false").equals("true");

    ttlSeconds = Integer.parseInt(properties.getProperty("couchbase.ttlSeconds", "0"));

    synchronized (INIT_COORDINATOR) {
      try {
        builder.connect(hostname, username, password)
            .create(true)
            .ttl(ttlSeconds)
            .durability(durabilityLevel)
            .keyspace(bucketName, scopeName, collectionName);
        if (capellaProject != null && capellaDatabase != null) {
          builder.capella(capellaProject, capellaDatabase);
        }
        db = builder.build();
        collection = db.getCollection();
      } catch (CouchbaseConnectException e) {
        throw new DBException(e);
      }
    }

    clientNumber = OPEN_CLIENTS.incrementAndGet();
  }

  /**
   * Checks the replicate parameter value.
   * @param property provided replicateTo parameter.
   * @return ReplicateTo value.
   */
  private static ReplicateTo parseReplicateTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return ReplicateTo.NONE;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
    default:
      throw new DBException("\"couchbase.replicateTo\" must be between 0 and 3");
    }
  }

  /**
   * Checks the persist parameter value.
   * @param property provided persistTo parameter.
   * @return PersistTo value.
   */
  private static PersistTo parsePersistTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return PersistTo.NONE;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
    default:
      throw new DBException("\"couchbase.persistTo\" must be between 0 and 4");
    }
  }

  /**
   * Checks the durability parameter.
   * @param property provided durability parameter.
   * @return DurabilityLevel value.
   */
  private static DurabilityLevel parseDurabilityLevel(final String property) throws DBException {

    int value = Integer.parseInt(property);

    switch(value){
    case 0:
      return DurabilityLevel.NONE;
    case 1:
      return DurabilityLevel.MAJORITY;
    case 2:
      return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
    case 3:
      return DurabilityLevel.PERSIST_TO_MAJORITY;
    default :
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
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

    if (collectKeyStats && runningClients == 0 && !loading) {
      KEY_STATS.info(statistics.getSummary());
    }

    for (Throwable t : errors) {
      LOGGER.error(t.getMessage(), t);
    }
  }

  private void waitForClients() {
    LOGGER.info(String.format("Client %d waiting for %d clients to finish", clientNumber, OPEN_CLIENTS.get()));
    while (OPEN_CLIENTS.get() > 0) {
      try {
        Thread.sleep(100L);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
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

  private MutationResult insertSwitch(Collection collection, String id, Object content) {
    if (doUpsert) {
      return upsertStub(collection, id, content);
    } else {
      return insertStub(collection, id, content);
    }
  }

  private MutationResult updateSwitch(Collection collection, String id, Object content) {
    if (doUpsert) {
      return upsertStub(collection, id, content);
    } else {
      return replaceStub(collection, id, content);
    }
  }

  private MutationResult insertStub(Collection collection, String id, Object content) {
    return collection.insert(id, content, dbInsertOptions);
  }

  private MutationResult upsertStub(Collection collection, String id, Object content) {
    return collection.upsert(id, content, dbUpsertOptions);
  }

  private MutationResult replaceStub(Collection collection, String id, Object content) {
    return collection.replace(id, content, dbReplaceOptions);
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
      String response = db.getString(formatId(table, key));
      if (response != null) {
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
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

  /**
   * Update record.
   * @param table The name of the table.
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record.
   */
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (Objects.requireNonNull(testMode) == TestType.ARRAY) {
        db.upsertArray(formatId(table, key), arrayKey, values);
      } else {
        db.upsert(formatId(table, key), values);
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  private Status updateDocument(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      return retryBlock(() -> {
          Collection collection = collectionEnabled ?
              bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
          Map<String, String> document = encode(values);
          document.put(recordId, String.valueOf(primaryKeySeq.incrementAndGet()));
          try {
            updateSwitch(collection, formatId(table, key), document);
          } catch (DocumentNotFoundException e) {
            insertSwitch(collection, formatId(table, key), document);
          }
          return Status.OK;
        });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  private Status updateArray(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      return retryBlock(() -> {
          Collection collection = collectionEnabled ?
              bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
          try {
            Map<String, String> document = encode(values);
            collection.mutateIn(formatId(table, key),
                Collections.singletonList(arrayAppend(arrayKey, Collections.singletonList(document))),
                dbMutateOptions);
          } catch (DocumentNotFoundException e) {
            List<Map<String, String>> value = new ArrayList<>();
            value.add(encode(values));
            Map<String, Object> document = new HashMap<>();
            document.put(recordId, String.valueOf(primaryKeySeq.incrementAndGet()));
            document.put(arrayKey, value);
            try {
              insertSwitch(collection, formatId(table, key), document);
            } catch (DocumentExistsException ex) {
              updateSwitch(collection, formatId(table, key), document);
            }
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
      if (Objects.requireNonNull(testMode) == TestType.ARRAY) {
        db.insertArray(formatId(table, key), arrayKey, values);
      } else {
        db.upsert(formatId(table, key), values);
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  public Status insertArray(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      return retryBlock(() -> {
          List<Map<String, String>> value = new ArrayList<>();
          value.add(encode(values));
          Map<String, Object> document = new HashMap<>();
          document.put(recordId, String.valueOf(primaryKeySeq.incrementAndGet()));
          document.put(arrayKey, value);
          Collection collection = collectionEnabled ?
              bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
          try {
            insertSwitch(collection, formatId(table, key), document);
          } catch (DocumentExistsException e) {
            updateSwitch(collection, formatId(table, key), document);
          }
          return Status.OK;
        });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("insert failed with exception :" + t);
      return Status.ERROR;
    }
  }

  private Status insertDocument(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      return retryBlock(() -> {
          Collection collection = collectionEnabled ?
              bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
          Map<String, String> document = encode(values);
          document.put(recordId, String.valueOf(primaryKeySeq.incrementAndGet()));
          try {
            insertSwitch(collection, formatId(table, key), document);
          } catch (DocumentExistsException e) {
            updateSwitch(collection, formatId(table, key), document);
          }
          return Status.OK;
        });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("insert failed with exception :" + t);
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
      db.remove(formatId(table, key));
      return Status.OK;
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
      if (fields == null || fields.isEmpty()) {
//        db.getAllDocs();
        return scanAllFields(table, startkey, recordcount, result);
      } else {
        return scanSpecificFields(table, startkey, recordcount, fields, result);
      }
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("scan failed with exception :" + t);
      return Status.ERROR;
    }
  }

  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation for all fields.
   * @param table The name of the table.
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   */
  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                               final Vector<HashMap<String, ByteIterator>> result) {

    final List<HashMap<String, ByteIterator>> data = new ArrayList<>(recordcount);
    final String query = "SELECT record_id FROM " + keyspaceName +
        " WHERE record_id >= \"$1\" ORDER BY record_id LIMIT $2";
    QueryOptions scanQueryOptions = QueryOptions.queryOptions();

    if (maxParallelism > 0) {
      scanQueryOptions.maxParallelism(maxParallelism);
    }

    cluster.reactive().query(query,
            scanQueryOptions
            .pipelineBatch(128)
            .pipelineCap(1024)
            .scanCap(1024)
            .adhoc(adhoc)
            .readonly(true)
            .parameters(JsonArray.from(numericId(startkey), recordcount)))
            .flatMapMany(ReactiveQueryResult::rowsAsObject)
              .onErrorResume(e -> {
                  LOGGER.error("Start Key: " + startkey + " Count: "
                      + recordcount + " Error:" + e.getClass() + " Info: " + e.getMessage());
                  return Mono.empty();
                })
              .map(row -> {
                  HashMap<String, ByteIterator> tuple = new HashMap<>();
                  tuple.put("record_id", new StringByteIterator(row.getString("record_id")));
                  return tuple;
                })
              .toStream()
              .forEach(data::add);

    result.addAll(data);
    return Status.OK;
  }

  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation only for a subset of the fields.
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */

  private Status scanSpecificFields(final String table, final String startkey, final int recordcount,
                                    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    final Collection collection = bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<>(recordcount);
    final String query =  "SELECT RAW meta().id FROM " + keyspaceName +
        " WHERE record_id >= $1 ORDER BY record_id LIMIT $2";
    final ReactiveCollection reactiveCollection = collection.reactive();
    QueryOptions scanQueryOptions = QueryOptions.queryOptions();

    if (maxParallelism > 0) {
      scanQueryOptions.maxParallelism(maxParallelism);
    }

    reactiveCluster.query(query,
            scanQueryOptions
            .adhoc(adhoc)
            .parameters(JsonArray.from(numericId(startkey), recordcount)))
        .flatMapMany(res -> res.rowsAs(String.class))
        .flatMap(id -> reactiveCollection
          .get(id, GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE)))
        .onErrorResume(e -> {
          LOGGER.error("scanSpecificFields: Start Key: " + startkey + " Count: "
              + recordcount + " Error:" + e.getClass() + " Info: " + e.getMessage());
          return Mono.empty();
        })
        .map(getResult -> {
            HashMap<String, ByteIterator> tuple = new HashMap<>();
            decodeStringSource(getResult.contentAs(String.class), fields, tuple);
            return tuple;
          })
        .toStream()
        .forEach(data::add);

    result.addAll(data);
    return Status.OK;
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

  /**
   * Helper function to convert the key to a numeric value.
   * @param key the key text
   * @return a string with non-numeric characters removed
   */
  private static String numericId(final String key) {
    return key.replaceAll("[^\\d.]", "");
  }

  /**
   * Helper function to generate the keyspace name.
   * @return a string with the computed keyspace name
   */
  private String getKeyspaceName() {
    if (scopeEnabled || collectionEnabled) {
      return this.bucketName + KEYSPACE_SEPARATOR + this.scopeName + KEYSPACE_SEPARATOR + this.collectionName;
    } else {
      return this.bucketName;
    }
  }

}
