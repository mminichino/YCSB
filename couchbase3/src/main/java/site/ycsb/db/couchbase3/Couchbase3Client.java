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

import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
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
import java.util.function.Consumer;

import com.couchbase.client.java.json.JsonArray;
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
  public static final String COUCHBASE_SSL_MODE = "couchbase.sslMode";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static volatile Cluster cluster;
  private static volatile Bucket bucket;
  private static volatile Collection collection;
  private static volatile ClusterEnvironment environment;
  private static String bucketName;
  private final ArrayList<Throwable> errors = new ArrayList<>();
  private boolean adhoc;
  private int maxParallelism;
  private static int ttlSeconds;
  private boolean arrayMode;
  private String arrayKey;
  private boolean collectStats;
  private static volatile DurabilityLevel durability = DurabilityLevel.NONE;

  @Override
  public void init() throws DBException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();
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

    String hostname = properties.getProperty(COUCHBASE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String username = properties.getProperty(COUCHBASE_USER, CouchbaseConnect.DEFAULT_USER);
    String password = properties.getProperty(COUCHBASE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    String scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    String collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    boolean sslMode = properties.getProperty(COUCHBASE_SSL_MODE, "false").equals("true");

    durability =
        setDurabilityLevel(Integer.parseInt(properties.getProperty("couchbase.durability", "0")));

    arrayMode = properties.getProperty("couchbase.mode", "DEFAULT").equals("ARRAY");
    arrayKey = properties.getProperty("subdoc.arrayKey", "DataArray");

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));

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
        if (environment == null) {
          Consumer<SecurityConfig.Builder> secConfiguration = securityConfig -> securityConfig
              .enableTls(sslMode)
              .enableHostnameVerification(false)
              .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);

          Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> ioConfig
              .numKvConnections(4)
              .networkResolution(NetworkResolution.AUTO)
              .enableMutationTokens(false);

          Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> timeoutConfig
              .kvTimeout(Duration.ofSeconds(2))
              .connectTimeout(Duration.ofSeconds(5))
              .queryTimeout(Duration.ofSeconds(75));

          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(timeOutConfiguration)
              .ioConfig(ioConfiguration)
              .securityConfig(secConfiguration)
              .build();
          cluster = Cluster.connect(connectString,
              ClusterOptions.clusterOptions(username, password).environment(environment));
          bucket = cluster.bucket(bucketName);
          collection = bucket.scope(scopeName).collection(collectionName);
        }
      } catch(Exception e) {
        logError(e, connectString);
      }
    }

    OPEN_CLIENTS.incrementAndGet();
  }

  private void logError(Exception error, String connectString) {
    LOGGER.error(String.format("Connection string: %s", connectString));
    LOGGER.error(cluster.environment().toString());
    LOGGER.error(cluster.diagnostics().endpoints().toString());
    LOGGER.error(error.getMessage(), error);
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
          collection.get(key, getOptions().transcoder(RawJsonTranscoder.INSTANCE));
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
        if (arrayMode) {
          upsertArray(key, arrayKey, encode(values));
        } else {
          collection.upsert(key, encode(values),
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
        if (arrayMode) {
          insertArray(key, arrayKey, encode(values));
        } else {
          collection.upsert(key, encode(values),
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
        collection.remove(key);
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
      return retryBlock(() -> {
        final String query = "select * from " + bucketName + " where meta().id >= \"$1\" limit $2;";
        cluster.reactive().query(query, queryOptions()
                .pipelineBatch(128)
                .pipelineCap(1024)
                .scanCap(1024)
                .readonly(true)
                .adhoc(adhoc)
                .maxParallelism(maxParallelism)
                .parameters(JsonArray.from(startkey, recordcount)))
            .flatMapMany(res -> res.rowsAsObject().parallel())
            .doOnError(e -> {
              throw new RuntimeException(e.getMessage());
            })
            .onErrorStop()
            .parallel()
            .subscribe();
        return Status.OK;
      });
    } catch (Throwable t) {
      errors.add(t);
      LOGGER.error("scan failed with exception :" + t);
      return Status.ERROR;
    }
  }
}
