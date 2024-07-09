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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

/**
 * A class that implements the Couchbase Java SDK to be used with YCSB.
 */
public class CouchbaseQuery extends DB {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseQuery");
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
  private static volatile ClusterEnvironment environment;
  private static String bucketName;
  private static String scopeName;
  private static String collectionName;
  private boolean adhoc;
  private int maxParallelism;
  private boolean analyticsMode;
  private static final AtomicLong recordNumber = new AtomicLong(0);

  @Override
  public void init() throws DBException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();
    String couchbasePrefix;

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        properties.load(propFile.openStream());
      } catch (IOException e) {
        throw new DBException(e);
      }
    }

    properties.putAll(getProperties());

    String hostname = properties.getProperty(COUCHBASE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String username = properties.getProperty(COUCHBASE_USER, CouchbaseConnect.DEFAULT_USER);
    String password = properties.getProperty(COUCHBASE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    boolean sslMode = properties.getProperty(COUCHBASE_SSL_MODE, "false").equals("true");
    boolean debug = properties.getProperty("couchbase.debug", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    analyticsMode = properties.getProperty("couchbase.analytics", "false").equals("true");

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));
    int kvEndpoints = Integer.parseInt(properties.getProperty("couchbase.kvEndpoints", "4"));

    long kvTimeout = Long.parseLong(properties.getProperty("couchbase.kvTimeout", "5"));
    long connectTimeout = Long.parseLong(properties.getProperty("couchbase.connectTimeout", "5"));
    long queryTimeout = Long.parseLong(properties.getProperty("couchbase.queryTimeout", "75"));

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
              .numKvConnections(kvEndpoints)
              .networkResolution(NetworkResolution.AUTO)
              .enableMutationTokens(false);

          Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> timeoutConfig
              .kvTimeout(Duration.ofSeconds(kvTimeout))
              .connectTimeout(Duration.ofSeconds(connectTimeout))
              .queryTimeout(Duration.ofSeconds(queryTimeout));

          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(timeOutConfiguration)
              .ioConfig(ioConfiguration)
              .securityConfig(secConfiguration)
              .build();

          cluster = Cluster.connect(connectString,
              ClusterOptions.clusterOptions(username, password).environment(environment));
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

  @Override
  public synchronized void cleanup() {
  }

  private static <T>T retryBlock(Callable<T> block) throws Exception {
    int retryCount = 10;
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      try {
        return block.call();
      } catch (Exception e) {
        LOGGER.debug("Retry count: {} error: {}", retryCount, e.getMessage(), e);
        if (retryNumber == retryCount) {
          throw e;
        } else {
          double factor = waitFactor * retryNumber;
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

  private Map<String, Object> decode(Map<String, ByteIterator> values) {
    Map<String, Object> result = new HashMap<>();
    values.entrySet()
        .parallelStream()
        .forEach(entry -> result.put(entry.getKey(), entry.getValue().toString()));
    return result;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (!analyticsMode) {
      return readQuery(key);
    } else {
      return readAnalytics(key);
    }
  }

  public Status readQuery(final String key) {
    TypeRef<Map<String, Object>> typeRef = new TypeRef<>() {};
    String statement = "SELECT * FROM " + keyspace() + " WHERE id = ?";
    try {
      return retryBlock(() -> {
        List<Map<String, Object>> results = cluster.reactive().query(statement, queryOptions()
                .pipelineBatch(128)
                .pipelineCap(1024)
                .scanCap(1024)
                .readonly(true)
                .maxParallelism(maxParallelism)
                .parameters(JsonArray.from(key))
                .retryStrategy(FailFastRetryStrategy.INSTANCE))
            .flatMapMany(result -> result.rowsAs(typeRef))
            .collectList()
            .block();
        if (LOGGER.isDebugEnabled() && results != null) {
          LOGGER.debug("Read Query Scanned {} records", results.size());
        }
        return Status.OK;
      });
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      LOGGER.error("read transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public Status readAnalytics(final String key) {
    TypeRef<Map<String, Object>> typeRef = new TypeRef<>() {};
    String statement = "SELECT * FROM " + keyspace() + " WHERE id = ?";
    try {
      return retryBlock(() -> {
        List<Map<String, Object>> results = cluster.reactive().analyticsQuery(statement, analyticsOptions()
                .readonly(true)
                .parameters(JsonArray.from(key))
                .retryStrategy(FailFastRetryStrategy.INSTANCE))
            .flatMapMany(result -> result.rowsAs(typeRef))
            .collectList()
            .block();
        if (LOGGER.isDebugEnabled() && results != null) {
          LOGGER.debug("Analytics Query Scanned {} records", results.size());
        }
        return Status.OK;
      });
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      LOGGER.error("read transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public Status insertRecordsQuery(String key, Map<String, ByteIterator> values) {
    String json;

    try {
      Map<String, Object> data = decode(values);
      data.put("id", key);
      data.put("record", recordNumber.incrementAndGet());
      ObjectMapper mapper = new ObjectMapper();
      json = mapper.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      LOGGER.error("Query Insert: Can not serialize values: {}", values);
      return Status.ERROR;
    }

    String statement = "UPSERT INTO " + keyspace() + " (KEY,VALUE) VALUES (?, " + json + ")";
    try {
      return retryBlock(() -> {
        cluster.query(statement, queryOptions()
            .pipelineBatch(128)
            .adhoc(adhoc)
            .maxParallelism(maxParallelism)
            .parameters(JsonArray.from(key)));
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public Status insertRecordsAnalytics(String key, Map<String, ByteIterator> values) {
    String json;

    try {
      Map<String, Object> data = decode(values);
      data.put("id", key);
      data.put("record", recordNumber.incrementAndGet());
      ObjectMapper mapper = new ObjectMapper();
      json = mapper.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      LOGGER.error("Analytics Insert: Can not serialize values: {}", values);
      return Status.ERROR;
    }

    String statement = "UPSERT INTO " + keyspace() + " (" + json + ")";
    try {
      return retryBlock(() -> {
        cluster.analyticsQuery(statement, analyticsOptions()
            .parameters(JsonArray.from(key)));
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
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
  public Status insert(final String table, final String key, Map<String, ByteIterator> values) {
    if (!analyticsMode) {
      return insertRecordsQuery(key, values);
    } else {
      return insertRecordsAnalytics(key, values);
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
    return insert(table, key, values);
  }

  private String keyspace() {
    if (!analyticsMode) {
      return bucketName + "." + scopeName + "." + collectionName;
    } else {
      return bucketName;
    }
  }

  /**
   * Remove a record.
   * @param table The name of the table.
   * @param key The record key of the record to delete.
   */
  @Override
  public Status delete(final String table, final String key) {
    String statement = "DELETE FROM " + keyspace() + " USE KEYS \"" + key + "\"";
    try {
      return retryBlock(() -> {
        QueryResult response = cluster.query(statement, queryOptions()
            .maxParallelism(maxParallelism)
            .retryStrategy(FailFastRetryStrategy.INSTANCE));
        return response.metaData().status() == QueryStatus.SUCCESS ? Status.OK : Status.ERROR;
      });
    } catch (DocumentNotFoundException e) {
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("read transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    if (!analyticsMode) {
      return scanQuery(startkey, recordcount);
    } else {
      return scanAnalytics(startkey, recordcount);
    }
  }

  public Status scanQuery(final String startkey, final int recordcount) {
    TypeRef<Map<String, Object>> typeRef = new TypeRef<>() {};
    final String statement = "SELECT * FROM " + keyspace() + " a WHERE a.record >= (SELECT RAW record FROM " + keyspace() + " b WHERE b.id = ?)[0] LIMIT ?";
    try {
      return retryBlock(() -> {
        List<Map<String, Object>> results = cluster.reactive().query(statement, queryOptions()
                .pipelineBatch(128)
                .pipelineCap(1024)
                .scanCap(1024)
                .readonly(true)
                .maxParallelism(maxParallelism)
                .parameters(JsonArray.from(startkey, recordcount))
                .retryStrategy(FailFastRetryStrategy.INSTANCE))
            .flatMapMany(result -> result.rowsAs(typeRef).parallel())
            .collectList()
            .block();
        if (LOGGER.isDebugEnabled() && results != null) {
          LOGGER.debug("Query Scanned {} records", results.size());
        }
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("query scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public Status scanAnalytics(final String startkey, final int recordcount) {
    TypeRef<Map<String, Object>> typeRef = new TypeRef<>() {};
    final String statement = "SELECT * FROM " + keyspace() + " a WHERE a.record >= (SELECT RAW record FROM " + keyspace() + " b WHERE b.id = ?)[0] LIMIT ?";
    try {
      return retryBlock(() -> {
        List<Map<String, Object>> results = cluster.reactive().analyticsQuery(statement, analyticsOptions()
                .readonly(true)
                .parameters(JsonArray.from(startkey, recordcount))
                .retryStrategy(FailFastRetryStrategy.INSTANCE))
            .flatMapMany(result -> result.rowsAs(typeRef).parallel())
            .collectList()
            .block();
        if (LOGGER.isDebugEnabled() && results != null) {
          LOGGER.debug("Analytics Scanned {} records", results.size());
        }
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("analytics scan transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }
}
