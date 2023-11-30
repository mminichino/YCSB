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
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.java.manager.bucket.*;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.google.gson.*;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private static Cluster cluster;
  private static Bucket bucket;
  private static Scope scope;
  private static Collection collection;
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  private static final Boolean DEFAULT_SSL_MODE = true;
  private static final String DEFAULT_PROJECT = null;
  private static final String DEFAULT_DATABASE = null;
  private static final String DEFAULT_SCOPE = "_default";
  private static final String DEFAULT_COLLECTION = "_default";
  private static final Object CONNECT_COORDINATOR = new Object();
  private String hostname;
  private String username;
  private String password;
  private String project;
  private String database;
  private String connectString;
  private boolean useExternal = false;
  private String bucketName;
  private String scopeName;
  private String collectionName;
  private Boolean useSsl;
  private BucketMode bucketMode;
  private String httpPrefix;
  private String couchbasePrefix;
  private String srvPrefix;
  private Integer adminPort;
  private Integer nodePort;
  private Integer eventingPort;
  private RESTInterface rest;
  private String rallyHost;
  private long memoryQuota;
  private long indexMemoryQuota;
  private long ftsMemoryQuota;
  private long cbasMemoryQuota;
  private long eventingMemoryQuota;
  private final List<String> rallyHostList = new ArrayList<>();
  private List<String> externalList = new ArrayList<>();
  private List<String> eventingList = new ArrayList<>();
  private final JsonArray hostMap = new JsonArray();
  private DiagnosticsResult diagnosticsResult;
  private BucketManager bucketMgr;
  private CouchbaseCapella capella;

  /**
   * Automatically Create Bucket.
   */
  public enum BucketMode {
    CREATE, PASSIVE
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

  public void connect() throws CouchbaseConnectException {
    synchronized (CONNECT_COORDINATOR) {
      StringBuilder connectBuilder = new StringBuilder();
      JsonObject clusterInfo;

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

      connectBuilder.append(couchbasePrefix);
      connectBuilder.append(hostname);

      connectString = connectBuilder.toString();

      Consumer<SecurityConfig.Builder> secConfiguration = securityConfig -> {
        securityConfig.enableTls(useSsl)
        .enableHostnameVerification(false)
        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
      };

      Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> {
        ioConfig.enableTcpKeepAlives(true)
        .tcpKeepAliveTime(Duration.ofSeconds(5))
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
          .retryStrategy(BestEffortRetryStrategy.INSTANCE)
          .ioConfig(ioConfiguration)
          .securityConfig(secConfiguration)
          .build();

      cluster = Cluster
          .connect(connectString,
              ClusterOptions.clusterOptions(username, password)
                  .environment(environment));

      cluster.waitUntilReady(Duration.ofSeconds(5));

      DiagnosticsResult diagnosticsResult = cluster.diagnostics();
      for (Map.Entry<ServiceType, List<EndpointDiagnostics>> service : diagnosticsResult.endpoints().entrySet()) {
        if (service.getKey() == ServiceType.KV) {
          for (EndpointDiagnostics ed : service.getValue()) {
            String[] endpoint = ed.remote().split(":", 2);
            rallyHostList.add(endpoint[0]);
          }
        }
      }

      rallyHost = rallyHostList.get(0);

      rest = new RESTInterface(rallyHost, username, password, useSsl, adminPort);
      if (project != null) {
        capella = new CouchbaseCapella(project, database);
      }

      try {
        clusterInfo = rest.getJSON("/pools/default");
      } catch (RESTException e) {
        throw new CouchbaseConnectException(e.getMessage());
      }

      memoryQuota = clusterInfo.get("memoryQuota").getAsLong();
      indexMemoryQuota = clusterInfo.get("indexMemoryQuota").getAsLong();
      ftsMemoryQuota = clusterInfo.get("ftsMemoryQuota").getAsLong();
      cbasMemoryQuota = clusterInfo.get("cbasMemoryQuota").getAsLong();
      eventingMemoryQuota = clusterInfo.get("eventingMemoryQuota").getAsLong();

      for (JsonElement node : clusterInfo.getAsJsonArray("nodes").asList()) {
        String hostEntry = node.getAsJsonObject().get("hostname").getAsString();
        String[] endpoint = hostEntry.split(":", 2);
        String hostname = endpoint[0];
        String external;

        if (node.getAsJsonObject().has("alternateAddresses")) {
          external = node.getAsJsonObject().get("alternateAddresses").getAsJsonObject().get("external")
              .getAsJsonObject().get("hostname").getAsString();
          Stream<String> stream = rallyHostList.parallelStream();
          boolean result = stream.anyMatch(e -> e.equals(external));
          if (!useExternal && result) {
            useExternal = true;
          }
        } else {
          external = null;
        }

        JsonArray services = node.getAsJsonObject().getAsJsonArray("services");

        JsonObject entry = new JsonObject();
        entry.addProperty("hostname", hostname);
        entry.addProperty("external", external);
        entry.add("services", services);

        hostMap.add(entry);
      }

      for (JsonElement node : hostMap.asList()) {
        List<JsonElement> services = node.getAsJsonObject().getAsJsonArray("services").asList();
        Stream<JsonElement> stream = services.parallelStream();
        boolean result = stream.anyMatch(e -> e.getAsString().equals("eventing"));
        if (result) {
          if (useExternal) {
            String external = node.getAsJsonObject().get("external").getAsString();
            eventingList.add(external);
          } else {
            String hostname = node.getAsJsonObject().get("hostname").getAsString();
            eventingList.add(hostname);
          }
        }
      }

      bucketMgr = cluster.buckets();
    }
  }

  public Collection keyspace(String bucket) throws CouchbaseConnectException {
    bucketName = bucket;
    scopeName = DEFAULT_SCOPE;
    collectionName = DEFAULT_COLLECTION;
    return connectKeyspace();
  }

  public Collection keyspace(String bucket, String scope) throws CouchbaseConnectException {
    bucketName = bucket;
    scopeName = scope;
    collectionName = DEFAULT_COLLECTION;
    return connectKeyspace();
  }

  public Collection keyspace(String bucket, String scope, String collection) throws CouchbaseConnectException {
    bucketName = bucket;
    scopeName = scope;
    collectionName = collection;
    return connectKeyspace();
  }

  public void bucketCreate(String bucket, int replicas) {
    long quota = ramQuotaCalc();
    bucketCreate(bucket, quota, replicas, StorageBackend.COUCHSTORE);
  }

  public void bucketCreate(String bucket, long quota, int replicas) {
    bucketCreate(bucket, quota, replicas, StorageBackend.COUCHSTORE);
  }

  public void bucketCreate(String bucket, int replicas, StorageBackend type) {
    long quota = ramQuotaCalc();
    bucketCreate(bucket, quota, replicas, type);
  }

  public void bucketCreate(String bucket, long quota, int replicas, StorageBackend type) {
    if (project != null && capella != null) {
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

  public long ramQuotaCalc() {
    long freeMemoryQuota = memoryQuota;
    Map<String, BucketSettings> buckets = bucketMgr.getAllBuckets();
    for (Map.Entry<String, BucketSettings> bucketEntry : buckets.entrySet()) {
      BucketSettings bucketSettings = bucketEntry.getValue();
      long ramQuota = bucketSettings.ramQuotaMB();
      freeMemoryQuota -= ramQuota;
    }
    return freeMemoryQuota;
  }

  public Collection connectKeyspace() throws CouchbaseConnectException {
    if (isBucket(bucketName)) {
      bucket = cluster.bucket(bucketName);
    } else {
      throw new CouchbaseConnectException("bucket does not exist");
    }
    bucket.waitUntilReady(Duration.ofSeconds(10));
    scope = bucket.scope(scopeName);
    collection = scope.collection(collectionName);
    return collection;
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
