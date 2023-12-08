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
import com.couchbase.client.core.error.UnambiguousTimeoutException;
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
import com.couchbase.client.java.query.QueryOptions;
import static com.couchbase.client.java.kv.MutateInSpec.arrayAppend;
//import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
//import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
//import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
//import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import com.google.gson.*;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

class CouchbaseConnectException extends Exception {
  public CouchbaseConnectException() {}

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
  private static Cluster cluster;
  private static Bucket bucket;
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  private static final Boolean DEFAULT_SSL_MODE = true;
  private static final String DEFAULT_PROJECT = null;
  private static final String DEFAULT_DATABASE = null;
  private static final BucketMode DEFAULT_BUCKET_MODE = BucketMode.PASSIVE;
  private static final String DEFAULT_SCOPE = "_default";
  private static final String DEFAULT_COLLECTION = "_default";
  private static final int DEFAULT_REPLICA_NUM = 1;
  private static final StorageBackend DEFAULT_STORAGE_BACKEND = StorageBackend.COUCHSTORE;
  private static final Object INIT_COORDINATOR = new Object();
  public final String hostname;
  public final String username;
  public final String password;
  public final String project;
  public final String database;
  public boolean external;
  public String bucketName;
  private String scopeName;
  private String collectionName;
  private long bucketQuota;
  private int replicaNum;
  private StorageBackend bucketType;
  public final Boolean useSsl;
  private String httpPrefix;
  private String couchbasePrefix;
  private String srvPrefix;
  public Integer adminPort;
  private Integer nodePort;
  private Integer eventingPort;
  private final BucketMode bucketMode;
  private JsonArray hostMap = new JsonArray();
  public String rallyHost;
  private boolean scopeEnabled;
  private boolean collectionEnabled;
  private final List<String> rallyHostList = new ArrayList<>();
  private final List<String> eventingList = new ArrayList<>();
  private DurabilityLevel durability = DurabilityLevel.NONE;
  private int ttlSeconds = 0;
  private GetOptions getOptions = GetOptions.getOptions();
  private RemoveOptions dbRemoveOptions = RemoveOptions.removeOptions();
  private InsertOptions dbInsertOptions = InsertOptions.insertOptions();
  private UpsertOptions dbUpsertOptions = UpsertOptions.upsertOptions();
  private ReplaceOptions dbReplaceOptions = ReplaceOptions.replaceOptions();
  private MutateInOptions dbMutateOptions = MutateInOptions.mutateInOptions();
  private QueryOptions queryOptions = QueryOptions.queryOptions();

  /**
   * Automatically Create Bucket.
   */
  public enum BucketMode {
    CREATE, PASSIVE
  }

  /**
   * Builder Class.
   */
  public static class CouchbaseBuilder {
    private String hostName = DEFAULT_HOSTNAME;
    private String userName = DEFAULT_USER;
    private String passWord = DEFAULT_PASSWORD;
    private Boolean sslMode = DEFAULT_SSL_MODE;
    private BucketMode bucketMode = DEFAULT_BUCKET_MODE;
    private String projectName = DEFAULT_PROJECT;
    private String databaseName = DEFAULT_DATABASE;
    private String bucketName;
    private String scopeName = DEFAULT_SCOPE;
    private String collectionName = DEFAULT_COLLECTION;
    private long bucketQuota = 0L;
    private int replicaNum = DEFAULT_REPLICA_NUM;
    private StorageBackend storageBackEnd = DEFAULT_STORAGE_BACKEND;
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

    public CouchbaseBuilder create(final Boolean mode) {
      if (mode) {
        this.bucketMode = BucketMode.CREATE;
      }
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

    public CouchbaseBuilder quota(final long value) {
      this.bucketQuota = value;
      return this;
    }

    public CouchbaseBuilder replicas(final int value) {
      this.replicaNum = value;
      return this;
    }

    public CouchbaseBuilder storage(final int value) {
      if (value == 1) {
        this.storageBackEnd = StorageBackend.MAGMA;
      } else {
        this.storageBackEnd = StorageBackend.COUCHSTORE;
      }
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

  private CouchbaseConnect(CouchbaseBuilder builder) throws CouchbaseConnectException {
    this.hostname = builder.hostName;
    this.username = builder.userName;
    this.password = builder.passWord;
    this.useSsl = builder.sslMode;
    this.project = builder.projectName;
    this.database = builder.databaseName;
    this.bucketMode = builder.bucketMode;
    this.durability = builder.durabilityLevel;
    this.ttlSeconds = builder.ttlSeconds;
    this.bucketName = builder.bucketName;
    this.scopeName = builder.scopeName;
    this.collectionName = builder.collectionName;
    this.replicaNum = builder.replicaNum;
    this.bucketType = builder.storageBackEnd;
    this.bucketQuota = builder.bucketQuota;
    this.scopeEnabled = !Objects.equals(scopeName, "_default");
    this.collectionEnabled = !Objects.equals(collectionName, "_default");
    this.connect();
  }

  public CouchbaseConnect(String hostname) throws CouchbaseConnectException {
    this.hostname = hostname;
    this.username = DEFAULT_USER;
    this.password = DEFAULT_PASSWORD;
    this.useSsl = DEFAULT_SSL_MODE;
    this.project = DEFAULT_PROJECT;
    this.database = DEFAULT_DATABASE;
    this.bucketMode = BucketMode.CREATE;
    this.connect();
  }

  public CouchbaseConnect(String hostname, String username, String password) throws CouchbaseConnectException {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = DEFAULT_SSL_MODE;
    this.project = DEFAULT_PROJECT;
    this.database = DEFAULT_DATABASE;
    this.bucketMode = BucketMode.CREATE;
    this.connect();
  }

  public CouchbaseConnect(String hostname, String username, String password, BucketMode mode)
      throws CouchbaseConnectException {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = DEFAULT_SSL_MODE;
    this.project = DEFAULT_PROJECT;
    this.database = DEFAULT_DATABASE;
    this.bucketMode = mode;
    this.connect();
  }

  public CouchbaseConnect(String hostname, String username, String password, String project, String database)
      throws CouchbaseConnectException {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.useSsl = DEFAULT_SSL_MODE;
    this.project = project;
    this.database = database;
    this.bucketMode = BucketMode.CREATE;
    this.connect();
  }

  public void connect() {
    if (useSsl) {
      httpPrefix = "https://";
      couchbasePrefix = "couchbases://";
      srvPrefix = "_couchbases._tcp.";
      adminPort = 18091;
      nodePort = 19102;
      eventingPort = 18096;
    } else {
      httpPrefix = "http://";
      couchbasePrefix = "couchbase://";
      srvPrefix = "_couchbase._tcp.";
      adminPort = 8091;
      nodePort = 9102;
      eventingPort = 8096;
    }

    Consumer<SecurityConfig.Builder> secConfiguration = securityConfig -> {
      securityConfig.enableTls(useSsl)
          .enableHostnameVerification(false)
          .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
    };

    Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> {
      ioConfig.numKvConnections(4)
          .networkResolution(NetworkResolution.AUTO);
    };

    Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> {
      timeoutConfig.kvTimeout(Duration.ofSeconds(2))
          .connectTimeout(Duration.ofSeconds(5))
          .queryTimeout(Duration.ofSeconds(75));
    };

    ClusterEnvironment environment = ClusterEnvironment
        .builder()
        .timeoutConfig(timeOutConfiguration)
        .ioConfig(ioConfiguration)
        .securityConfig(secConfiguration)
        .build();

    cluster = Cluster
        .connect(hostname,
            ClusterOptions.clusterOptions(username, password)
                .environment(environment));

    try {
      cluster.waitUntilReady(Duration.ofSeconds(10));
    } catch (UnambiguousTimeoutException e) {
      throw new RuntimeException(String.format("timeout waiting for cluster with node %s", hostname));
    }
    bucket = cluster.bucket(bucketName);
    try {
      hostMap = getClusterInfo();
      rallyHost = getRallyHost();
      external = getExternalFlag();
    } catch (CouchbaseConnectException e) {
      throw new RuntimeException(e);
    }
  }

  private String buildHostList() {
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

  public String getRallyHost() {
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

  public boolean getExternalFlag() {
    Stream<JsonElement> stream = hostMap.asList().parallelStream();
    return stream.filter(e -> e.getAsJsonObject().get("services").getAsJsonArray()
            .contains(JsonParser.parseString("kv")))
        .map(e -> e.getAsJsonObject().get("useExternal").getAsBoolean())
        .findFirst()
        .orElse(false);
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

  public void keyspace(String bucket) throws CouchbaseConnectException {
    bucketName = bucket;
    scopeName = DEFAULT_SCOPE;
    collectionName = DEFAULT_COLLECTION;
    connectKeyspace();
  }

  public void keyspace(String bucket, String scope) throws CouchbaseConnectException {
    bucketName = bucket;
    scopeName = scope;
    collectionName = DEFAULT_COLLECTION;
    connectKeyspace();
  }

  public void keyspace(String bucket, String scope, String collection) throws CouchbaseConnectException {
    bucketName = bucket;
    scopeName = scope;
    collectionName = collection;
    connectKeyspace();
  }

  public void createBucket(String bucket, int replicas) {
    long quota = ramQuotaCalc();
    cbCreateBucket(bucket, quota, replicas, StorageBackend.COUCHSTORE);
  }

  public void createBucket(String bucket, long quota, int replicas) {
    cbCreateBucket(bucket, quota, replicas, StorageBackend.COUCHSTORE);
  }

  public void createBucket(String bucket, int replicas, StorageBackend type) {
    long quota = ramQuotaCalc();
    cbCreateBucket(bucket, quota, replicas, type);
  }

  public void cbCreateBucket(String bucket, long quota, int replicas, StorageBackend type) {
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

        BucketManager bucketMgr = cluster.buckets();
        bucketMgr.createBucket(bucketSettings);
      } catch (BucketExistsException e) {
        //ignore
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
        BucketManager bucketMgr = cluster.buckets();
        bucketMgr.dropBucket(bucket);
      } catch (BucketNotFoundException e) {
        //ignore
      }
    }
  }

  public Boolean isBucket(String bucket) {
    try {
      BucketManager bucketMgr = cluster.buckets();
      bucketMgr.getBucket(bucket);
      return true;
    } catch (BucketNotFoundException e) {
      return false;
    }
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

  private void setOptions() {
    if (durability != DurabilityLevel.NONE) {
      dbInsertOptions = dbInsertOptions.durability(durability);
      dbUpsertOptions = dbUpsertOptions.durability(durability);
      dbReplaceOptions = dbReplaceOptions.durability(durability);
      dbRemoveOptions = dbRemoveOptions.durability(durability);
    }

    if (ttlSeconds > 0) {
      dbInsertOptions = dbInsertOptions.expiry(Duration.ofSeconds(ttlSeconds));
      dbUpsertOptions = dbUpsertOptions.expiry(Duration.ofSeconds(ttlSeconds));
      dbReplaceOptions = dbReplaceOptions.expiry(Duration.ofSeconds(ttlSeconds));
      dbMutateOptions = dbMutateOptions.expiry(Duration.ofSeconds(ttlSeconds));
    }
  }

  public void connectKeyspace() {
    bucket = cluster.bucket(bucketName);
//    scope = bucket.scope(scopeName);
//    collection = scope.collection(collectionName);
    setOptions();
//    return collection;
  }

  public Collection getCollection() {
    return collectionEnabled ?
        bucket.scope(this.scopeName).collection(this.collectionName) : bucket.defaultCollection();
  }

  public Bucket getBucket() {
    return bucket;
  }

  public String getString(String id) throws Exception {
    Collection collection = getCollection();
    return RetryLogic.retryBlock(() -> {
      GetResult result;
      try {
        result = collection.get(id, getOptions().transcoder(RawJsonTranscoder.INSTANCE));
        return result.contentAs(String.class);
      } catch (DocumentNotFoundException e) {
        return null;
      }
    });
  }

  public JsonObject getJSON(String id) throws Exception {
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
      collection.upsert(id, content, dbUpsertOptions);
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
            dbMutateOptions);
        return result.cas();
      } catch (DocumentNotFoundException e) {
        JsonObject document = new JsonObject();
        JsonArray subDocArray = new JsonArray();
        Gson gson = new Gson();
        String subDoc = gson.toJson(content);
        subDocArray.add(gson.fromJson(subDoc, JsonObject.class));
        document.add(arrayKey, subDocArray);
        MutationResult result = collection.upsert(id, document, dbUpsertOptions);
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
      MutationResult result = collection.upsert(id, document, dbUpsertOptions);
      return result.cas();
    });
  }

  public long remove(String id) throws Exception {
    Collection collection = getCollection();
    return RetryLogic.retryBlock(() -> {
      MutationResult result = collection.remove(id, dbRemoveOptions);
      return result.cas();
    });
  }

  public List<JsonObject> getAllDocs(long offset, long limit) throws Exception {
    return RetryLogic.retryBlock(() -> {
      ReactiveCluster reactor = cluster.reactive();
      final String query = String.format("select * from ycsb offset %d limit %d;", offset, limit);
      final List<JsonObject> data = new ArrayList<>();
      reactor.query(query, queryOptions().maxParallelism(1).adhoc(false))
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

  public Boolean isEventingFunction(String name) throws CouchbaseConnectException {
    String eventingHost = eventingList.get(0);
    RESTInterface eventing = new RESTInterface(eventingHost, username, password, useSsl, eventingPort);
    String endpoint = "/api/v1/functions/" + name;

    try {
      eventing.getJSON(endpoint);
      return true;
    } catch (RESTException e) {
      return false;
    }
  }

  public Boolean isEventingFunctionDeployed(String name) throws CouchbaseConnectException {
    String eventingHost = eventingList.get(0);
    RESTInterface eventing = new RESTInterface(eventingHost, username, password, useSsl, eventingPort);
    String endpoint = "/api/v1/functions/" + name;

    try {
      JsonObject result = eventing.getJSON(endpoint);
      return result.get("settings").getAsJsonObject().get("deployment_status").getAsBoolean();
    } catch (RESTException e) {
      return false;
    }
  }

  public void deployEventingFunction(String scriptFile, String metaBucket)
      throws CouchbaseConnectException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL inputFile = classloader.getResource(scriptFile);

    String fileName = inputFile != null ? inputFile.getFile() : null;

    if (fileName == null) {
      throw new CouchbaseConnectException("Can not find script file");
    }

    File funcFile = new File(fileName);
    String[] fileParts = funcFile.getName().split("\\.");
    String name = fileParts[0];

    byte[] encoded;
    try {
      encoded = Files.readAllBytes(Paths.get(funcFile.getAbsolutePath()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String fileContents = new String(encoded, StandardCharsets.UTF_8);

    JsonObject parameters = new JsonObject();
    parameters.addProperty("appcode", fileContents);
    JsonObject depConfig = new JsonObject();
    JsonObject bucketConfig = new JsonObject();
    bucketConfig.addProperty("alias", "collection");
    bucketConfig.addProperty("bucket_name", bucketName);
    bucketConfig.addProperty("scope_name", scopeName);
    bucketConfig.addProperty("collection_name", collectionName);
    bucketConfig.addProperty("access", "rw");
    JsonArray buckets = new JsonArray();
    buckets.add(bucketConfig);
    depConfig.add("buckets", buckets);
    depConfig.addProperty("source_bucket", bucketName);
    depConfig.addProperty("source_scope", scopeName);
    depConfig.addProperty("source_collection", collectionName);
    depConfig.addProperty("metadata_bucket", metaBucket);
    depConfig.addProperty("metadata_scope", "_default");
    depConfig.addProperty("metadata_collection", "_default");
    parameters.add("depcfg", depConfig);
    parameters.addProperty("enforce_schema", false);
    parameters.addProperty("appname", name);
    JsonObject settingsConfig = new JsonObject();
    settingsConfig.addProperty("dcp_stream_boundary", "everything");
    settingsConfig.addProperty("description", "Auto Added Function");
    settingsConfig.addProperty("execution_timeout", 60);
    settingsConfig.addProperty("language_compatibility", "6.6.2");
    settingsConfig.addProperty("log_level", "INFO");
    settingsConfig.addProperty("n1ql_consistency", "none");
    settingsConfig.addProperty("processing_status", false);
    settingsConfig.addProperty("timer_context_size", 1024);
    settingsConfig.addProperty("worker_count", 16);
    parameters.add("settings", settingsConfig);
    JsonObject functionConfig = new JsonObject();
    functionConfig.addProperty("bucket", "*");
    functionConfig.addProperty("scope", "*");
    parameters.add("function_scope", functionConfig);

    String eventingHost = eventingList.get(0);
    RESTInterface eventing = new RESTInterface(eventingHost, username, password, useSsl, eventingPort);

    if (!isEventingFunction(name)) {
      try {
        String endpoint = "/api/v1/functions/" + name;
        eventing.postJSON(endpoint, parameters);
      } catch (RESTException e) {
        throw new RuntimeException(e);
      }
    }

    if (!isEventingFunctionDeployed(name)) {
      try {
        String endpoint = "/api/v1/functions/" + name + "/deploy";
        eventing.postEndpoint(endpoint);
      } catch (RESTException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
