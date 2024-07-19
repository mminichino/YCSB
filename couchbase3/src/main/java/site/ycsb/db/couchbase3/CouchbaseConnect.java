/*
 * Couchbase Connect
 */

package site.ycsb.db.couchbase3;

import ch.qos.logback.classic.Logger;
import com.couchbase.client.core.config.AlternateAddress;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.config.PortInfo;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.java.http.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.manager.analytics.AnalyticsDataType;
import com.couchbase.client.java.manager.bucket.*;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.query.CollectionQueryIndexManager;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import com.couchbase.client.java.manager.analytics.AnalyticsIndexManager;
import com.couchbase.client.java.analytics.AnalyticsResult;
import static com.couchbase.client.java.manager.analytics.CreateDatasetAnalyticsOptions.createDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDatasetAnalyticsOptions.dropDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateIndexAnalyticsOptions.createIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropIndexAnalyticsOptions.dropIndexAnalyticsOptions;
import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.java.kv.MutateInSpec.arrayAppend;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.*;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Couchbase Connection Utility.
 */
public final class CouchbaseConnect {
  private static final Logger LOGGER =
      (Logger) LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseConnect");
  private volatile Cluster cluster;
  private volatile Bucket bucket;
  private volatile Scope scope;
  private volatile Collection collection;
  private volatile ClusterEnvironment environment;
  private volatile BucketManager bucketMgr;
  private volatile AnalyticsIndexManager analytics;
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
  private JsonArray hostMap = new JsonArray();
  private JsonObject clusterInfo = new JsonObject();
  private final boolean scopeEnabled;
  private final boolean collectionEnabled;
  private boolean analyticsEnabled;
  private boolean columnar;
  private final DurabilityLevel durability;
  private final int ttlSeconds;
  private boolean communityEdition = false;

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
    private boolean columnar = false;
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

    public CouchbaseBuilder columnar(final boolean columnar) {
      this.columnar = columnar;
      return this;
    }

    public CouchbaseBuilder keyspace(final String bucket, final String scope, final String collection) {
      this.bucketName = bucket;
      this.scopeName = scope;
      this.collectionName = collection;
      return this;
    }

    public CouchbaseConnect build() {
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
    columnar = builder.columnar;
    scopeEnabled = !Objects.equals(scopeName, "_default");
    collectionEnabled = !Objects.equals(collectionName, "_default");
    analyticsEnabled = false;
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
              .kvTimeout(Duration.ofSeconds(5))
              .connectTimeout(Duration.ofSeconds(15))
              .analyticsTimeout(Duration.ofSeconds(360))
              .queryTimeout(Duration.ofSeconds(75));

          environment = ClusterEnvironment
              .builder()
              .timeoutConfig(timeOutConfiguration)
              .ioConfig(ioConfiguration)
              .securityConfig(secConfiguration)
              .build();
          cluster = Cluster.connect(connectString,
              ClusterOptions.clusterOptions(username, password).environment(environment));
          cluster.waitUntilReady(Duration.ofSeconds(15));
          if (!columnar) {
            bucketMgr = cluster.buckets();
            try {
              if (bucketName != null) {
                bucketMgr.getBucket(bucketName);
                bucket = cluster.bucket(bucketName);
              }
            } catch (BucketNotFoundException ignored) {
            }
            getClusterInfo();
          }
        }
      } catch(Exception e) {
        logError(e, connectString);
      }
    }
  }

  public void disconnect() {
    collection = null;
    bucketMgr = null;
    bucket = null;
    if (cluster != null) {
      cluster.disconnect(Duration.ofSeconds(15));
    }
    if (environment != null) {
      environment.shutdown();
    }
    cluster = null;
    environment = null;
    project = null;
    database = null;
    hostMap = new JsonArray();
    clusterInfo = new JsonObject();
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

  public String getBucketName() {
    return bucketName;
  }

  public String getScopeName() {
    return scopeName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public CouchbaseHttpClient getHttpClient() {
    return cluster.httpClient();
  }

  private void logError(Exception error, String connectString) {
    LOGGER.error(String.format("Connection string: %s", connectString));
    LOGGER.error(cluster.environment().toString());
    LOGGER.error(cluster.diagnostics().endpoints().toString());
    LOGGER.error(error.getMessage(), error);
  }

  private void getClusterInfo() {
    List<Map.Entry<String, AlternateAddress>> nodeExt = cluster.core().clusterConfig().globalConfig().portInfos()
            .stream()
            .map(PortInfo::alternateAddresses)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(Collectors.toList());

    if (!nodeExt.isEmpty()) {
      external = true;
    }

    HttpResponse response = cluster.httpClient().get(
            HttpTarget.manager(),
            HttpPath.of("/pools/default"));

    Gson gson = new Gson();
    clusterInfo = gson.fromJson(response.contentAsString(), JsonObject.class);

    communityEdition = !clusterInfo.has("cbasMemoryQuota");

    for (JsonElement node : clusterInfo.getAsJsonArray("nodes").asList()) {
      String hostEntry = node.getAsJsonObject().get("hostname").getAsString();
      String[] endpoint = hostEntry.split(":", 2);
      String hostname = endpoint[0];
      JsonArray services = node.getAsJsonObject().getAsJsonArray("services");

      JsonObject entry = new JsonObject();
      entry.addProperty("hostname", hostname);
      entry.add("services", services);
      boolean result = services.asList().stream().anyMatch(e -> e.getAsString().equals("cbas"));
      if (result) {
        analyticsEnabled = true;
        analytics = new AnalyticsIndexManager(cluster);
      }

      hostMap.add(entry);
    }
  }

  public long getIndexNodeCount() {
    Stream<JsonElement> stream = hostMap.asList().parallelStream();
    return stream.filter(e -> e.getAsJsonObject().get("services").getAsJsonArray()
            .contains(JsonParser.parseString("index")))
        .count();
  }

  private long getMemQuota() {
    return clusterInfo.get("memoryQuota").getAsLong();
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
        LOGGER.info(String.format("Bucket %s already exists in cluster", bucket));
      }
    }
    Bucket check = cluster.bucket(bucket);
    check.waitUntilReady(Duration.ofSeconds(15));
  }

  public void createAnalyticsCollection(String bucketName) {
    analytics.createDataset(bucketName, bucketName, createDatasetAnalyticsOptions().ignoreIfExists(true));
  }

  public void dropAnalyticsCollection(String bucketName) {
    analytics.dropDataset(bucketName, dropDatasetAnalyticsOptions().ignoreIfNotExists(true));
  }

  public void createAnalyticsIntIndex(String bucketName, String fieldName) {
    Map<String,AnalyticsDataType> fieldMap = new HashMap<>();
    fieldMap.put(fieldName, AnalyticsDataType.INT64);
    String indexName = bucketName + "_" + fieldName + "_idx";
    analytics.createIndex(indexName, bucketName, fieldMap, createIndexAnalyticsOptions().ignoreIfExists(true));
  }

  public void createAnalyticsStrIndex(String bucketName, String fieldName) {
    Map<String,AnalyticsDataType> fieldMap = new HashMap<>();
    fieldMap.put(fieldName, AnalyticsDataType.STRING);
    String indexName = bucketName + "_" + fieldName + "_idx";
    analytics.createIndex(indexName, bucketName, fieldMap, createIndexAnalyticsOptions().ignoreIfExists(true));
  }

  public void dropAnalyticsIndex(String bucketName, String fieldName) {
    String indexName = bucketName + "_" + fieldName + "_idx";
    analytics.dropIndex(indexName, bucketName, dropIndexAnalyticsOptions().ignoreIfNotExists(true));
  }

  public void createScope(String bucketName, String scopeName) {
    if (Objects.equals(scopeName, "_default")) {
      return;
    }
    bucketMgr.getBucket(bucketName);
    bucket = cluster.bucket(bucketName);
    CollectionManager collectionManager = bucket.collections();
    try {
      collectionManager.createScope(scopeName);
    } catch (ScopeExistsException e) {
      LOGGER.info(String.format("Scope %s already exists in cluster", scopeName));
    }
  }

  public void createCollection(String bucketName, String scopeName, String collectionName) {
    if (Objects.equals(collectionName, "_default")) {
      return;
    }
    bucketMgr.getBucket(bucketName);
    bucket = cluster.bucket(bucketName);
    CollectionManager collectionManager = bucket.collections();
    try {
      collectionManager.createCollection(scopeName, collectionName);
    } catch (CollectionExistsException e) {
      LOGGER.info(String.format("Collection %s already exists in cluster", collectionName));
    }
  }

  public boolean isAnalyticsEnabled() {
    return analyticsEnabled;
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

  public Boolean isBucket(String bucket) {
    try {
      bucketMgr.getBucket(bucket);
      return true;
    } catch (BucketNotFoundException e) {
      return false;
    }
  }

  public int getIndexReplicaCount() {
    int indexNodes = (int) getIndexNodeCount();
    if (communityEdition) {
      return 0;
    } else if (indexNodes <= 4) {
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

  public JsonNode document(String id) {
    TypeRef<JsonNode> typeRef = new TypeRef<>() {};
    try {
      return RetryLogic.retryBlock(() -> {
        try {
          GetResult result =  collection.get(id);
          return result.contentAs(typeRef);
        } catch (DocumentNotFoundException e) {
          return null;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  public List<JsonNode> runQuery(String statement) {
    TypeRef<JsonNode> typeRef = new TypeRef<>() {};
    Bucket bucket = cluster.bucket(bucketName);
    Scope scope = bucket.scope(scopeName);
    return scope.reactive().query(statement, queryOptions())
        .flatMapMany(result -> result.rowsAs(typeRef))
        .collectList()
        .block();
  }

  public List<ObjectNode> analyticsQuery(String statement) {
    TypeRef<ObjectNode> typeRef = new TypeRef<>() {};
    Bucket bucket = cluster.bucket(bucketName);
    Scope scope = bucket.scope(scopeName);
    try {
      AnalyticsResult result = cluster.analyticsQuery(statement, analyticsOptions());
      return result.rowsAs(typeRef);
    } catch (Throwable t) {
      LOGGER.error("analytics query exception: {}", t.getMessage(), t);
      return null;
    }
  }

  public List<ObjectNode> analyticsScopeQuery(String statement) {
    TypeRef<ObjectNode> typeRef = new TypeRef<>() {};
    Bucket bucket = cluster.bucket(bucketName);
    Scope scope = bucket.scope(scopeName);
    try {
      AnalyticsResult result = scope.analyticsQuery(statement, analyticsOptions()
          .timeout(Duration.ofSeconds(360))
          .priority(true));
      return result.rowsAs(typeRef);
    } catch (Throwable t) {
      LOGGER.error("analytics query exception: {}", t.getMessage(), t);
      return null;
    }
  }

  public List<ObjectNode> analyticsScopeQuery(String statement, List<String> parameters) {
    TypeRef<ObjectNode> typeRef = new TypeRef<>() {};
    Bucket bucket = cluster.bucket(bucketName);
    Scope scope = bucket.scope(scopeName);
    try {
      AnalyticsResult result = scope.analyticsQuery(statement,
          analyticsOptions().parameters(com.couchbase.client.java.json.JsonArray.from(parameters))
              .timeout(Duration.ofSeconds(360))
              .priority(true));
      return result.rowsAs(typeRef);
    } catch (Throwable t) {
      LOGGER.error("analytics query exception: {}", t.getMessage(), t);
      return null;
    }
  }

  public List<JsonNode> analyticsRESTQuery(String statement) {
    CouchbaseHttpClient client = cluster.httpClient();
    String endpoint = "/analytics/service";
    HttpResponse response;

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode body = mapper.createObjectNode();

    try {
      body.put("statement", statement);
      body.put("query_context", "default:" + bucketName + "." + scopeName);
      response = client.post(
          HttpTarget.analytics(),
          HttpPath.of(endpoint),
          HttpPostOptions.httpPostOptions().timeout(Duration.ofSeconds(360))
              .body(HttpBody.json(body.toString())));
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }

    try {
      if (response == null || !response.success()) {
        LOGGER.error(response != null ? response.contentAsString() : "no response text (was null)");
        return null;
      }
      List<JsonNode> rows = new ArrayList<>();
      JsonNode results = mapper.readTree(response.contentAsString());
      if (results.has("results")) {
        for (JsonNode row : results.get("results")) {
          rows.add(row);
        }
      }
      return rows;
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
