/*
 * Couchbase Connect
 */

package site.ycsb.db.couchbase3;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.time.Duration;

/**
 * Couchbase Connection Utility.
 */
@SuppressWarnings("InstantiationOfUtilityClass")
public final class CouchbaseConnect {
  private static CouchbaseConnect instance;
  private CouchbaseConnect() {
  }
  private static Cluster cluster;
  private static final String DEFAULT_USER = "Administrator";
  private static final String DEFAULT_PASSWORD = "password";
  private static final String DEFAULT_HOSTNAME = "127.0.0.1";
  private static final Boolean DEFAULT_SSL_MODE = false;
  private static final String DEFAULT_PROJECT = "project";
  private static final String DEFAULT_DATABASE = "database";
  private static final Boolean DEFAULT_EXTERNAL_MODE = false;
  private static final Object CONNECT_COORDINATOR = new Object();

  public static synchronized CouchbaseConnect getInstance() {
    if (instance == null) {
      instance = new CouchbaseConnect();
    }
    return instance;
  }

  public static void connect() {
    connectCluster(DEFAULT_HOSTNAME, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SSL_MODE,
        DEFAULT_PROJECT, DEFAULT_DATABASE, DEFAULT_EXTERNAL_MODE);
  }

  public static void connect(String hostname) {
    connectCluster(hostname, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SSL_MODE, DEFAULT_PROJECT, DEFAULT_DATABASE, DEFAULT_EXTERNAL_MODE);
  }

  public static void connect(String hostname, String username, String password) {
    connectCluster(hostname, username, password, true, DEFAULT_PROJECT, DEFAULT_DATABASE, DEFAULT_EXTERNAL_MODE);
  }

  public static void connect(String hostname, String username, String password, String project, String database) {
    connectCluster(hostname, username, password, true, project, database, DEFAULT_EXTERNAL_MODE);
  }

  public static void connectCluster(String hostname, String username, String password, Boolean ssl,
                                    String project, String database, Boolean external) {
    synchronized (CONNECT_COORDINATOR) {
      if (cluster == null) {
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

        String connectionString = connectBuilder.toString();

        ClusterEnvironment environment = ClusterEnvironment.builder()
            .timeoutConfig(t -> TimeoutConfig
                .kvTimeout(Duration.ofSeconds(5))
                .connectTimeout(Duration.ofSeconds(20))
                .queryTimeout(Duration.ofSeconds(75))
            )
            .retryStrategy(BestEffortRetryStrategy.INSTANCE)
            .ioConfig(i -> IoConfig.enableTcpKeepAlives(true)
                .tcpKeepAliveTime(Duration.ofSeconds(5))
                .networkResolution(network)
            )
            .securityConfig(s -> SecurityConfig.enableTls(ssl)
                .enableHostnameVerification(false)
                .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)
            )
            .build();
        cluster = Cluster
            .connect(connectionString,
                ClusterOptions.clusterOptions(username, password)
                    .environment(environment));
      }
    }
  }

  public static Collection keyspace(String bucket) {
    return connectKeyspace(bucket, "_default", "_default");
  }

  public static Collection keyspace(String bucket, String scope, String collection) {
    return connectKeyspace(bucket, scope, collection);
  }

  public static Collection connectKeyspace(String bucket, String scope, String collection) {
    Bucket bucketObj = cluster.bucket(bucket);
    bucketObj.waitUntilReady(Duration.ofSeconds(10));
    Scope scopeObj = bucketObj.scope(scope);
    return scopeObj.collection(collection);
  }
}
