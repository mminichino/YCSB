package site.ycsb.db.couchbase3;

import site.ycsb.TestCleanup;

import java.util.Properties;

/**
 * Clean Cluster after Testing.
 */
public class CouchbaseQueryCleanup extends TestCleanup {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_PROJECT = "couchbase.project";
  public static final String CLUSTER_DATABASE = "couchbase.database";

  @Override
  public void testClean(Properties properties) {
    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "ycsb");
    String clusterProject = properties.getProperty(CLUSTER_PROJECT, null);
    String clusterDatabase = properties.getProperty(CLUSTER_DATABASE, null);

    System.err.println("Starting test cleanup");

    clusterClean(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket,
        clusterProject, clusterDatabase);
  }

  private static void clusterClean(String host, String user, String password, boolean ssl, String bucket,
                                   String project, String database) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;

    try {
      dbBuilder.connect(host, user, password)
          .ssl(ssl)
          .bucket(bucket);
      if (project != null && database != null) {
        dbBuilder.capella(project, database);
      }
      db = dbBuilder.build();
      if (db.isAnalyticsEnabled()) {
        System.err.printf("Removing analytics collection %s\n", bucket);
        db.dropAnalyticsCollection(bucket);
      }
      System.err.printf("Removing bucket %s on cluster:[%s]\n", bucket, host);
      db.dropBucket(bucket);
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
