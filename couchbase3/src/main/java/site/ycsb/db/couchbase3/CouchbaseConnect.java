/*
 * Couchbase Connect
 */

package site.ycsb.db.couchbase3;

import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.manager.bucket.*;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.java.manager.query.CollectionQueryIndexManager;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import static com.couchbase.client.java.kv.MutateInSpec.arrayAppend;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import com.google.gson.*;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

class CouchbaseConnectException extends Exception {

  public CouchbaseConnectException(String message) {
    super(message);
  }
}

/**
 * Couchbase Connection Utility.
 */
public final class CouchbaseConnect {
  private static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.couchbase.CouchbaseConnect");
  private static volatile Cluster cluster;
  private static volatile Bucket bucket;
  private static volatile Scope scope;
  private static volatile Collection collection;
  private static volatile ClusterEnvironment environment;
  private static volatile BucketManager bucketMgr;
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final Boolean DEFAULT_SSL_MODE = true;
  public static final String DEFAULT_SSL_SETTING = "true";
  private static final String DEFAULT_PROJECT = null;
  private static final String DEFAULT_DATABASE = null;
  private static final String DEFAULT_SCOPE = "_default";
  private static final String DEFAULT_COLLECTION = "_default";
  private static final Object STARTUP_COORDINATOR = new Object();
  private final String hostname;
  private final String username;
  private final String password;
  private String project;
  private String database;
  private boolean external;
  private final String bucketName;
  private final String scopeName;
  private final String collectionName;
  private final Boolean useSsl;
  public int adminPort;
  public int eventingPort;
  private static JsonArray hostMap = new JsonArray();
  private static String rallyHost;
  private static boolean scopeEnabled;
  private static boolean collectionEnabled;
  private static List<String> rallyHostList = new ArrayList<>();
  private static List<String> eventingList = new ArrayList<>();
  private static DurabilityLevel durability = DurabilityLevel.NONE;
  private static int ttlSeconds = 0;

  /**
   * Builder Class.
   */
  public static class CouchbaseBuilder {
    private String hostName = DEFAULT_HOSTNAME;
    private String userName = DEFAULT_USER;
    private String passWord = DEFAULT_PASSWORD;
    private Boolean sslMode = DEFAULT_SSL_MODE;
    private String projectName = DEFAULT_PROJECT;
    private String databaseName = DEFAULT_DATABASE;
    private String bucketName;
    private String scopeName = DEFAULT_SCOPE;
    private String collectionName = DEFAULT_COLLECTION;
    private int ttlSeconds = 0;
    private DurabilityLevel durabilityLevel = DurabilityLevel.NONE;

    public CouchbaseBuilder durability(final int value) {
      switch(value){
        case 1:
          this.durabilityLevel = DurabilityLevel.MAJORITY;
          break;
        case 2:
          this.durabilityLevel = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
          break;
        case 3:
          this.durabilityLevel = DurabilityLevel.PERSIST_TO_MAJORITY;
          break;
        default :
          this.durabilityLevel = DurabilityLevel.NONE;
      }
      return this;
    }

    public CouchbaseBuilder ttl(int value) {
      this.ttlSeconds = value;
      return this;
    }

    public CouchbaseBuilder host(final String name) {
      this.hostName = name;
      return this;
    }

    public CouchbaseBuilder user(final String name) {
      this.userName = name;
      return this;
    }

    public CouchbaseBuilder password(final String name) {
      this.passWord = name;
      return this;
    }

    public CouchbaseBuilder connect(final String host, final String user, final String password) {
      this.hostName = host;
      this.userName = user;
      this.passWord = password;
      return this;
    }

    public CouchbaseBuilder ssl(final Boolean mode) {
      this.sslMode = mode;
      return this;
    }

    public CouchbaseBuilder capella(final String project, final String database) {
      this.projectName = project;
      this.databaseName = database;
      return this;
    }

    public CouchbaseBuilder bucket(final String name) {
      this.bucketName = name;
      return this;
    }

    public CouchbaseBuilder scope(final String name) {
      this.scopeName = name;
      return this;
    }

    public CouchbaseBuilder collection(final String name) {
      this.collectionName = name;
      return this;
    }

    public CouchbaseBuilder keyspace(final String bucket, final String scope, final String collection) {
      this.bucketName = bucket;
      this.scopeName = scope;
      this.collectionName = collection;
      return this;
    }

    public CouchbaseConnect build() throws CouchbaseConnectException {
      return new CouchbaseConnect(this);
    }
  }

  private CouchbaseConnect(CouchbaseBuilder builder) {
    hostname = builder.hostName;
    username = builder.userName;
    password = builder.passWord;
    useSsl = builder.sslMode;
    project = builder.projectName;
    database = builder.databaseName;
    durability = builder.durabilityLevel;
    ttlSeconds = builder.ttlSeconds;
    bucketName = builder.bucketName;
    scopeName = builder.scopeName;
    collectionName = builder.collectionName;
    scopeEnabled = !Objects.equals(scopeName, "_default");
    collectionEnabled = !Objects.equals(collectionName, "_default");
    connect();
  }

  public void connect() {
    String couchbasePrefix;

    if (useSsl) {
      couchbasePrefix = "couchbases://";
      adminPort = 18091;
      eventingPort = 18096;
    } else {
      couchbasePrefix = "couchbase://";
      adminPort = 8091;
      eventingPort = 8096;
    }

    String connectString = couchbasePrefix + hostname;

    synchronized (STARTUP_COORDINATOR) {
      try {
        if (environment == null) {
          Consumer<SecurityConfig.Builder> secConfiguration = securityConfig -> securityConfig
              .enableTls(useSsl)
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
          cluster.waitUntilReady(Duration.ofSeconds(10));
          bucket = cluster.bucket(bucketName);
          bucketMgr = cluster.buckets();
          hostMap = getClusterInfo();
          rallyHost = getRallyHost();
          external = getExternalFlag();
        }
      } catch(Exception e) {
        e.printStackTrace();
        logError(e, connectString);
      }
    }
  }

  public void disconnect() {
    collection = null;
    bucketMgr = null;
    bucket = null;
    if (cluster != null) {
      cluster.disconnect(Duration.ofSeconds(10));
    }
    if (environment != null) {
      environment.shutdown();
    }
    cluster = null;
    environment = null;
    rallyHost = null;
    rallyHostList = new ArrayList<>();
    project = null;
    database = null;
  }

  public String rallyHostValue() {
    return rallyHost;
  }

  public String hostValue() {
    return hostname;
  }

  public String userValue() {
    return username;
  }

  public String passwordValue() {
    return password;
  }

  public boolean externalValue() {
    return external;
  }

  public boolean sslValue() {
    return useSsl;
  }

  public int getAdminPort() {
    return adminPort;
  }

  public int getEventingPort() {
    return eventingPort;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getScopeName() {
    return scopeName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  private static void logError(Exception error, String connectString) {
    LOGGER.error(String.format("Connection string: %s", connectString));
    LOGGER.error(cluster.environment().toString());
    LOGGER.error(cluster.diagnostics().endpoints().toString());
    LOGGER.error(error.getMessage(), error);
  }

  private static String buildHostList() {
    if (rallyHostList.isEmpty() || rallyHost == null) {
      DiagnosticsResult diagnosticsResult = cluster.diagnostics();
      for (Map.Entry<ServiceType, List<EndpointDiagnostics>> service : diagnosticsResult.endpoints().entrySet()) {
        if (service.getKey() == ServiceType.KV) {
          for (EndpointDiagnostics ed : service.getValue()) {
            if (ed.remote() != null && !ed.remote().isEmpty()) {
              String[] endpoint = ed.remote().split(":", 2);
              rallyHostList.add(endpoint[0]);
            }
          }
        }
      }
      return rallyHostList.get(0);
    }
    return null;
  }

  private JsonArray getClusterInfo() throws CouchbaseConnectException {
    JsonObject clusterInfo;
    String rallyHost = buildHostList();
    JsonArray hostMap = new JsonArray();

    if (rallyHost == null) {
      throw new CouchbaseConnectException("can not determine rally host");
    }

    try {
      RESTInterface rest = new RESTInterface(rallyHost, username, password, useSsl, adminPort);
      clusterInfo = rest.getJSON("/pools/default");
    } catch (RESTException e) {
      throw new CouchbaseConnectException(e.getMessage());
    }

    for (JsonElement node : clusterInfo.getAsJsonArray("nodes").asList()) {
      String hostEntry = node.getAsJsonObject().get("hostname").getAsString();
      String[] endpoint = hostEntry.split(":", 2);
      String hostname = endpoint[0];
      String external;
      boolean useExternal = false;

      if (node.getAsJsonObject().has("alternateAddresses")) {
        external = node.getAsJsonObject().get("alternateAddresses").getAsJsonObject().get("external")
            .getAsJsonObject().get("hostname").getAsString();
        Stream<String> stream = rallyHostList.parallelStream();
        boolean result = stream.anyMatch(e -> e.equals(external));
        if (result) {
          useExternal = true;
        }
      } else {
        external = null;
      }

      JsonArray services = node.getAsJsonObject().getAsJsonArray("services");

      JsonObject entry = new JsonObject();
      entry.addProperty("hostname", hostname);
      entry.addProperty("external", external);
      entry.addProperty("useExternal", useExternal);
      entry.add("services", services);

      hostMap.add(entry);
    }
    return hostMap;
  }

  public static String getRallyHost() {
    Stream<JsonElement> stream = hostMap.asList().parallelStream();
    return stream.filter(e -> e.getAsJsonObject().get("services").getAsJsonArray()
            .contains(JsonParser.parseString("kv")))
        .map(e -> {
          if (e.getAsJsonObject().get("useExternal").getAsBoolean()) {
            return e.getAsJsonObject().get("external").getAsString();
          } else {
            return e.getAsJsonObject().get("hostname").getAsString();
          }
        })
        .findFirst()
        .orElse(null);
  }

  public static boolean getExternalFlag() {
    Stream<JsonElement> stream = hostMap.asList().parallelStream();
    return stream.filter(e -> e.getAsJsonObject().get("services").getAsJsonArray()
            .contains(JsonParser.parseString("kv")))
        .map(e -> e.getAsJsonObject().get("useExternal").getAsBoolean())
        .findFirst()
        .orElse(false);
  }

  public static long getIndexNodeCount() {
    Stream<JsonElement> stream = hostMap.asList().parallelStream();
    return stream.filter(e -> e.getAsJsonObject().get("services").getAsJsonArray()
            .contains(JsonParser.parseString("index")))
        .count();
  }

  private long getMemQuota() {
    RESTInterface rest = new RESTInterface(rallyHost, username, password, useSsl, adminPort);
    JsonObject clusterInfo;
    try {
      clusterInfo = rest.getJSON("/pools/default");
      return clusterInfo.get("memoryQuota").getAsLong();
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
  }

  public void createBucket(String bucket, int replicas) {
    long quota = ramQuotaCalc();
    bucketCreate(bucket, quota, replicas, 0);
  }

  public void createBucket(String bucket, long quota, int replicas) {
    bucketCreate(bucket, quota, replicas, 0);
  }

  public void createBucket(String bucket, int replicas, int bucketType) {
    long quota = ramQuotaCalc();
    bucketCreate(bucket, quota, replicas, bucketType);
  }

  public void createBucket(String bucket, long quota, int replicas, int bucketType) {
    bucketCreate(bucket, quota, replicas, bucketType);
  }

  public void bucketCreate(String bucket, long quota, int replicas, int bucketType) {
    StorageBackend type;
    if (bucketType == 1) {
      type = StorageBackend.MAGMA;
    } else {
      type = StorageBackend.COUCHSTORE;
    }
    if (project != null && database != null) {
      CouchbaseCapella capella = new CouchbaseCapella(project, database);
      capella.createBucket(bucket, quota, replicas, type);
    } else {
      try {
        BucketSettings bucketSettings = BucketSettings.create(bucket)
            .flushEnabled(false)
            .replicaIndexes(true)
            .ramQuotaMB(quota)
            .numReplicas(replicas)
            .bucketType(BucketType.COUCHBASE)
            .storageBackend(type)
            .conflictResolutionType(ConflictResolutionType.SEQUENCE_NUMBER);

        bucketMgr.createBucket(bucketSettings);
      } catch (BucketExistsException e) {
        //ignore
        LOGGER.info(String.format("Bucket %s already exists in cluster", bucket));
      }
    }
    Bucket check = cluster.bucket(bucket);
    check.waitUntilReady(Duration.ofSeconds(10));
  }

  public void dropBucket(String bucket) {
    if (project != null && database != null) {
      CouchbaseCapella capella = new CouchbaseCapella(project, database);
      capella.dropBucket(bucket);
    } else {
      try {
        bucketMgr.dropBucket(bucket);
      } catch (BucketNotFoundException e) {
        //ignore
      }
    }
  }

  public static Boolean isBucket(String bucket) {
    try {
      bucketMgr.getBucket(bucket);
      return true;
    } catch (BucketNotFoundException e) {
      return false;
    }
  }

  public static int getIndexReplicaCount() {
    int indexNodes = (int) getIndexNodeCount();
    if (indexNodes <= 4) {
      return indexNodes - 1;
    } else {
      return 3;
    }
  }

  public void createPrimaryIndex() {
    int replicaCount = getIndexReplicaCount();
    if (collection == null) {
      connectKeyspace();
    }
    CollectionQueryIndexManager queryIndexMgr = collection.queryIndexes();
    CreatePrimaryQueryIndexOptions options = CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions()
        .deferred(false)
        .numReplicas(replicaCount)
        .ignoreIfExists(true);
    queryIndexMgr.createPrimaryIndex(options);
  }

  public void createFieldIndex(String field) {
    int replicaCount = getIndexReplicaCount();
    if (collection == null) {
      connectKeyspace();
    }
    CollectionQueryIndexManager queryIndexMgr = collection.queryIndexes();
    CreateQueryIndexOptions options = CreateQueryIndexOptions.createQueryIndexOptions()
        .deferred(false)
        .numReplicas(replicaCount)
        .ignoreIfExists(true);

    String indexName = "idx_" + field.replaceAll("\\(\\).", "");
    queryIndexMgr.createIndex(indexName, Collections.singletonList(field), options);
    queryIndexMgr.watchIndexes(Collections.singletonList(indexName), Duration.ofSeconds(10));
  }

  public long ramQuotaCalc() {
    long freeMemoryQuota = getMemQuota();
    BucketManager bucketMgr = cluster.buckets();
    Map<String, BucketSettings> buckets = bucketMgr.getAllBuckets();
    for (Map.Entry<String, BucketSettings> bucketEntry : buckets.entrySet()) {
      BucketSettings bucketSettings = bucketEntry.getValue();
      long ramQuota = bucketSettings.ramQuotaMB();
      freeMemoryQuota -= ramQuota;
    }
    return freeMemoryQuota;
  }

  public void connectKeyspace() {
    bucket = cluster.bucket(bucketName);
    scope = bucket.scope(scopeName);
    collection = scope.collection(collectionName);
  }

  public Scope getScope() {
    return scopeEnabled ?
        bucket.scope(scopeName) : bucket.defaultScope();
  }

  public Collection getCollection() {
    return collectionEnabled ?
        bucket.scope(scopeName).collection(collectionName) : bucket.defaultCollection();
  }

  public Bucket getBucket() {
    return bucket;
  }

  public JsonObject get(String id) throws Exception {
    Collection collection = getCollection();
    return RetryLogic.retryBlock(() -> {
      GetResult result;
      try {
        result = collection.get(id, getOptions().transcoder(RawJsonTranscoder.INSTANCE));
        String json = result.contentAs(String.class);
        return new Gson().fromJson(json, JsonObject.class);
      } catch (DocumentNotFoundException e) {
        return null;
      }
    });
  }

  public void upsert(String id, Object content) throws Exception {
    Collection collection = getCollection();
    try {
      collection.upsert(id, content, upsertOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
  }

  public long upsertArray(String id, String arrayKey, Object content) throws Exception {
    Collection collection = getCollection();
    return RetryLogic.retryBlock(() -> {
      try {
        MutationResult result = collection.mutateIn(id,
            Collections.singletonList(arrayAppend(arrayKey, Collections.singletonList(content))),
            mutateInOptions().expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
        return result.cas();
      } catch (DocumentNotFoundException e) {
        JsonObject document = new JsonObject();
        JsonArray subDocArray = new JsonArray();
        Gson gson = new Gson();
        String subDoc = gson.toJson(content);
        subDocArray.add(gson.fromJson(subDoc, JsonObject.class));
        document.add(arrayKey, subDocArray);
        MutationResult result = collection.upsert(id, document, upsertOptions()
            .expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
        return result.cas();
      }
    });
  }

  public long insertArray(String id, String arrayKey, Object content) throws Exception {
    Collection collection = getCollection();
    return RetryLogic.retryBlock(() -> {
      JsonObject document = new JsonObject();
      JsonArray subDocArray = new JsonArray();
      Gson gson = new Gson();
      String subDoc = gson.toJson(content);
      subDocArray.add(gson.fromJson(subDoc, JsonObject.class));
      document.add(arrayKey, subDocArray);
      MutationResult result = collection.upsert(id, document, upsertOptions()
          .expiry(Duration.ofSeconds(ttlSeconds)).durability(durability));
      return result.cas();
    });
  }

  public long remove(String id) throws Exception {
    Collection collection = getCollection();
    return RetryLogic.retryBlock(() -> {
      MutationResult result = collection.remove(id);
      return result.cas();
    });
  }

  public List<JsonObject> query(String queryString) throws Exception {
    return RetryLogic.retryBlock(() -> {
      final List<JsonObject> data = new ArrayList<>();
      cluster.reactive().query(queryString)
          .flatMapMany(res -> res.rowsAs(String.class))
          .doOnError(e -> {
            throw new RuntimeException(e.getMessage());
          })
          .onErrorStop()
          .map(getResult -> new Gson().fromJson(getResult, JsonObject.class))
          .toStream()
          .forEach(data::add);
          return data;
      });
  }
}
