package site.ycsb.db.couchbase3;

import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

import ch.qos.logback.classic.Logger;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import static com.couchbase.client.java.kv.MutateInSpec.arrayAppend;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.query.CollectionQueryIndexManager;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import site.ycsb.*;
import site.ycsb.Record;

/**
 * A class that implements the Couchbase Java SDK to be used with YCSB.
 */
public class CouchbaseSQLClient extends SQLDB {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseSQLClient");
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
  private static volatile CollectionManager collectionManager;
  private static volatile ClusterEnvironment environment;
  private static String bucketName;
  private static String scopeName;
  private boolean adhoc;
  private int maxParallelism;
  private static int ttlSeconds;
  private boolean arrayMode;
  private String arrayKey;
  private Transcoder transcoder;
  private Class<?> contentType;
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
    String collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    boolean sslMode = properties.getProperty(COUCHBASE_SSL_MODE, "false").equals("true");
    boolean debug = getProperties().getProperty("couchbase.debug", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    boolean nativeCodec = getProperties().getProperty("couchbase.codec", "ycsb").equals("native");

    if (nativeCodec) {
      transcoder = RawJsonTranscoder.INSTANCE;
      contentType = String.class;
    } else {
      transcoder = MapTranscoder.INSTANCE;
      contentType = Map.class;
    }

    durability =
        setDurabilityLevel(Integer.parseInt(properties.getProperty("couchbase.durability", "0")));

    arrayMode = properties.getProperty("couchbase.mode", "default").equals("array");
    arrayKey = properties.getProperty("subdoc.arrayKey", "DataArray");

    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "0"));
    int kvEndpoints = Integer.parseInt(properties.getProperty("couchbase.kvEndpoints", "4"));

    long kvTimeout = Long.parseLong(properties.getProperty("couchbase.kvTimeout", "5"));
    long connectTimeout = Long.parseLong(properties.getProperty("couchbase.connectTimeout", "5"));
    long queryTimeout = Long.parseLong(properties.getProperty("couchbase.queryTimeout", "75"));

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
          bucket = cluster.bucket(bucketName);
          collectionManager = bucket.collections();
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

  public void createFieldIndex(String collectionName, String field) {
    Collection collection = bucket.scope(scopeName).collection(collectionName);
    CollectionQueryIndexManager queryIndexMgr = collection.queryIndexes();
    CreateQueryIndexOptions options = CreateQueryIndexOptions.createQueryIndexOptions()
        .deferred(false)
        .numReplicas(1)
        .ignoreIfExists(true);

    String indexName = "idx_" + field.replaceAll("\\(\\).", "");
    queryIndexMgr.createIndex(indexName, Collections.singletonList(field), options);
    queryIndexMgr.watchIndexes(Collections.singletonList(indexName), Duration.ofSeconds(10));
  }

  public Status createTable(String table, Map<String, DataType> columns, Set<String> keys) {
    try {
      LOGGER.debug("Creating collection: {}", table);
      collectionManager.createCollection(scopeName, table);
      for (String key : keys) {
        createFieldIndex(table, key);
      }
      return Status.OK;
    } catch (CollectionExistsException e) {
      LOGGER.debug("Collection {} already exists", table);
      return Status.OK;
    } catch (Throwable t) {
      LOGGER.error("createTable transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public Status dropTable(String table, Map<String, DataType> columns, Set<String> keys) {
    collectionManager.dropCollection(scopeName, table);
    return Status.OK;
  }

  /**
   * Perform select.
   */
  @Override
  public List<Map<String, ?>> select(String statement, ArrayList<Object> parameters) {
    TypeRef<Map<String, ?>> typeRef = new TypeRef<>() {};
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(statement);
    }
    try {
      QueryResult result = cluster.query(statement, queryOptions()
              .adhoc(adhoc)
              .maxParallelism(maxParallelism)
              .parameters(JsonArray.from(parameters))
              .retryStrategy(FailFastRetryStrategy.INSTANCE));
      return result.rowsAs(typeRef);
    } catch (Throwable t) {
      LOGGER.error("select transaction exception: {}", t.getMessage(), t);
      return new ArrayList<>();
    }
  }

  public Status insert(Record data) {
    try {
      String table = data.tableName();
      String key = String.join("::", data.getKeyValues());
      Collection collection = bucket.scope(scopeName).collection(table);
      return retryBlock(() -> {
        collection.upsert(key, data.contents(),
            upsertOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
        return Status.OK;
      });
    } catch (Throwable t) {
      LOGGER.error("insert transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  public Status update(String statement, ArrayList<Object> parameters) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(statement);
    }
    try {
      QueryResult result = cluster.query(statement, queryOptions()
          .adhoc(adhoc)
          .maxParallelism(maxParallelism)
          .parameters(JsonArray.from(parameters))
          .retryStrategy(FailFastRetryStrategy.INSTANCE));
      return result.metaData().status() == QueryStatus.SUCCESS ? Status.OK : Status.ERROR;
    } catch (Throwable t) {
      LOGGER.error("update transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
   * query records.
   */
  @Override
  public Status query(String statement) {
    try {
      cluster.query(statement, queryOptions()
          .pipelineBatch(128)
          .pipelineCap(1024)
          .scanCap(1024)
          .readonly(true)
          .adhoc(adhoc)
          .maxParallelism(maxParallelism)
          .retryStrategy(FailFastRetryStrategy.INSTANCE));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(statement);
      }
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      LOGGER.error("read transaction exception: {}", t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
   * Delete a record.
   */
  @Override
  public Status delete(String table, String statement) {
    try {
      cluster.query(statement, queryOptions()
          .pipelineBatch(128)
          .pipelineCap(1024)
          .scanCap(1024)
          .readonly(true)
          .adhoc(adhoc)
          .maxParallelism(maxParallelism)
          .retryStrategy(FailFastRetryStrategy.INSTANCE));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(statement);
      }
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      LOGGER.error("read transaction exception: {}", t.getMessage(), t);
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
    values.entrySet()
        .parallelStream()
        .forEach(entry -> result.put(entry.getKey(), entry.getValue().toString()));
    return result;
  }
}
