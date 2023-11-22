/*
 * Couchbase Connect
 */

package site.ycsb.db.couchbase3;

import com.couchbase.client.core.deps.com.google.gson.JsonParser;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.core.deps.com.google.gson.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Duration;
import java.util.function.Consumer;

final class DCPDocument {
  private final String key;
  private final JsonObject document;

  public DCPDocument(String key, JsonObject document) {
    this.key = key;
    this.document = document;
  }

  public String getKey() {
    return key;
  }

  public JsonObject getDocument() {
    return document;
  }
}

/**
 * Couchbase Connection Utility.
 */
public final class CouchbaseConnect {
  private static CouchbaseConnect instance;
  private CouchbaseConnect() {}
  private static Cluster pCluster;
  private static Cluster xCluster;
  private static Client pClient;
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  private static final Boolean DEFAULT_SSL_MODE = false;
  private static final String DEFAULT_PROJECT = "project";
  private static final String DEFAULT_DATABASE = "database";
  private static final Boolean DEFAULT_EXTERNAL_MODE = false;
  private static final Object CONNECT_COORDINATOR = new Object();
  private String primaryUser;
  private String primaryPassword;
  private String primaryConString;
  private String primaryBucket;
  private String primaryScope;
  private String primaryCollection;
  private Boolean primarySslSetting;
  private String xdcrUser;
  private String xdcrPassword;
  private String xdcrConString;
  private String xdcrBucket;
  private String xdcrScope;
  private String xdcrCollection;
  private Boolean xdcrSslSetting;

  /**
   * Connect To Primary or XDCR.
   */
  public enum ConnectMode {
    PRIMARY, XDCR
  }

  public static CouchbaseConnect getInstance() {
    if (instance == null) {
      instance = new CouchbaseConnect();
    }
    return instance;
  }

  public void connect() {
    connectCluster(DEFAULT_HOSTNAME, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SSL_MODE,
        DEFAULT_PROJECT, DEFAULT_DATABASE, DEFAULT_EXTERNAL_MODE, ConnectMode.PRIMARY);
  }

  public void connect(String hostname) {
    connectCluster(hostname, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SSL_MODE,
        DEFAULT_PROJECT, DEFAULT_DATABASE, DEFAULT_EXTERNAL_MODE, ConnectMode.PRIMARY);
  }

  public void connect(String hostname, String username, String password, ConnectMode mode) {
    connectCluster(hostname, username, password, true,
        DEFAULT_PROJECT, DEFAULT_DATABASE, DEFAULT_EXTERNAL_MODE, mode);
  }

  public void connect(String hostname, String username, String password, String project, String database) {
    connectCluster(hostname, username, password, true,
        project, database, DEFAULT_EXTERNAL_MODE, ConnectMode.PRIMARY);
  }

  public void connectCluster(String hostname, String username, String password, Boolean ssl,
                                    String project, String database, Boolean external, ConnectMode mode) {
    synchronized (CONNECT_COORDINATOR) {
      if (pCluster == null) {
        StringBuilder connectBuilder = new StringBuilder();
        NetworkResolution network;
        String httpPrefix;
        String couchbasePrefix;
        String srvPrefix;
        String adminPort;
        String nodePort;

        if (ssl) {
          httpPrefix = "https://";
          couchbasePrefix = "couchbases://";
          srvPrefix = "_couchbases._tcp.";
          adminPort = "18091";
          nodePort = "19102";
        } else {
          httpPrefix = "http://";
          couchbasePrefix = "couchbase://";
          srvPrefix = "_couchbase._tcp.";
          adminPort = "8091";
          nodePort = "9102";
        }

        if (external) {
          network = NetworkResolution.EXTERNAL;
        } else {
          network = NetworkResolution.AUTO;
        }

        connectBuilder.append(couchbasePrefix);
        connectBuilder.append(hostname);

        primaryConString = connectBuilder.toString();

        System.out.println(primaryConString);
        System.out.println(ssl);

        Consumer<SecurityConfig.Builder> secConfiguration = securityConfig -> {
          securityConfig.enableTls(ssl)
          .enableHostnameVerification(false)
          .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
        };

        Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> {
          ioConfig.enableTcpKeepAlives(true)
          .tcpKeepAliveTime(Duration.ofSeconds(5))
          .networkResolution(network);
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
        System.out.println(environment.toString());
        Cluster cluster = Cluster
            .connect(primaryConString,
                ClusterOptions.clusterOptions(username, password)
                    .environment(environment));

        if (mode == ConnectMode.PRIMARY) {
          pCluster = cluster;
          primaryUser = username;
          primaryPassword = password;
          primarySslSetting = ssl;
        } else {
          xCluster = cluster;
          xdcrUser = username;
          xdcrPassword = password;
          xdcrSslSetting = ssl;
        }
      }
    }
  }

  public Client dcpConnect() {
    try (Client client = Client.builder()
        .connectionString(primaryConString)
        .bucket(primaryBucket)
        .credentials(primaryUser, primaryPassword)
        .build()) {
      return client;
    }
  }

  public List<DCPDocument> getDocs(String bucket, ConnectMode mode) {
    List<DCPDocument> documents = new ArrayList<>();
    String conString = mode == ConnectMode.PRIMARY ? primaryConString : xdcrConString;
    String username = mode == ConnectMode.PRIMARY ? primaryUser : xdcrUser;
    String password = mode == ConnectMode.PRIMARY ? primaryPassword : xdcrPassword;
    Boolean ssl = mode == ConnectMode.PRIMARY ? primarySslSetting : xdcrSslSetting;
    boolean collectionEnabled = mode == ConnectMode.PRIMARY ?
        !primaryCollection.equals("_default") : !xdcrCollection.equals("_default");

    Consumer<com.couchbase.client.dcp.SecurityConfig.Builder> secClientConfig = securityConfig -> {
      securityConfig.enableTls(ssl)
          .enableHostnameVerification(false)
          .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
    };

    Client client = Client.builder()
        .connectionString(conString)
        .bucket(bucket)
        .securityConfig(secClientConfig)
        .credentials(username, password)
        .build();

    client.controlEventHandler((flowController, event) -> {
      flowController.ack(event);
      event.release();
    });

    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        CollectionIdAndKey key = MessageUtil.getCollectionIdAndKey(event, collectionEnabled);
        String content = DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8);
        JsonObject jsonObject = JsonParser.parseString(content).getAsJsonObject();
        DCPDocument entry = new DCPDocument(key.key(), jsonObject);
        documents.add(entry);
      }
      event.release();
    });

    // Connect the sockets
    client.connect().block();

    // Initialize the state (start now, never stop)
    client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).block();

    // Start streaming on all partitions
    client.startStreaming().block();

    // Sleep and wait until the DCP stream has caught up with the time where we said "now".
    while (!client.sessionState().isAtEnd()) {
      try {
        Thread.sleep(500L);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    // Proper Shutdown
    client.disconnect().block();
    return documents;
  }

  public Collection keyspace(String bucket, ConnectMode mode) {
    return connectKeyspace(bucket, "_default", "_default", mode);
  }

  public Collection keyspace(String bucket, String scope, String collection, ConnectMode mode) {
    return connectKeyspace(bucket, scope, collection, mode);
  }

  public Collection connectKeyspace(String bucket, String scope, String collection, ConnectMode mode) {
    Cluster cluster;
    if (mode == ConnectMode.PRIMARY) {
      cluster = pCluster;
      primaryBucket = bucket;
      primaryScope = scope;
      primaryCollection = collection;
    } else {
      cluster = xCluster;
      xdcrBucket = bucket;
      xdcrScope = scope;
      xdcrCollection = collection;
    }
    Bucket bucketObj = cluster.bucket(bucket);
    bucketObj.waitUntilReady(Duration.ofSeconds(10));
    Scope scopeObj = bucketObj.scope(scope);
    primaryBucket = bucket;
    return scopeObj.collection(collection);
  }
}
