package site.ycsb.db.couchbase3;

import site.ycsb.TestCleanup;

import java.util.Properties;

/**
 * Clean Cluster after Testing.
 */
public class CouchbaseTestCleanup extends TestCleanup {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_PROJECT = "couchbase.project";
  public static final String CLUSTER_DATABASE = "couchbase.database";
  public static final String CLUSTER_EVENTING = "couchbase.eventing";
  public static final String XDCR_HOST = "xdcr.hostname";
  public static final String XDCR_USER = "xdcr.username";
  public static final String XDCR_PASSWORD = "xdcr.password";
  public static final String XDCR_SSL = "xdcr.sslMode";
  public static final String XDCR_BUCKET = "xdcr.bucket";
  public static final String XDCR_PROJECT = "xdcr.project";
  public static final String XDCR_DATABASE = "xdcr.database";
  public static final String XDCR_EVENTING = "xdcr.eventing";

  @Override
  public void testClean(Properties properties) {
    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "ycsb");
    String clusterProject = properties.getProperty(CLUSTER_PROJECT, null);
    String clusterDatabase = properties.getProperty(CLUSTER_DATABASE, null);
    String clusterEventing = properties.getProperty(CLUSTER_EVENTING, null);

    String xdcrHost = properties.getProperty(XDCR_HOST, null);
    String xdcrUser = properties.getProperty(XDCR_USER, CouchbaseConnect.DEFAULT_USER);
    String xdcrPassword = properties.getProperty(XDCR_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean xdcrSsl = properties.getProperty(XDCR_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String xdcrBucket = properties.getProperty(XDCR_BUCKET, "ycsb");
    String xdcrProject = properties.getProperty(XDCR_PROJECT, null);
    String xdcrDatabase = properties.getProperty(XDCR_DATABASE, null);
    String xdcrEventing = properties.getProperty(XDCR_EVENTING, null);

    System.err.println("Starting test cleanup");

    if (xdcrHost != null) {
      replicationClean(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket,
          xdcrHost, xdcrUser, xdcrPassword, xdcrSsl, xdcrBucket);
      clusterClean(xdcrHost, xdcrUser, xdcrPassword, xdcrSsl, xdcrBucket,
          xdcrProject, xdcrDatabase, xdcrEventing);
    }

    clusterClean(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket,
        clusterProject, clusterDatabase, clusterEventing);
  }

  private static void clusterClean(String host, String user, String password, boolean ssl, String bucket,
                                   String project, String database, String eventing) {
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
      if (eventing != null) {
        System.err.printf("Removing eventing bucket on cluster:[%s]\n", host);
        db.dropBucket("eventing");
      }
      System.err.printf("Removing bucket %s on cluster:[%s]\n", bucket, host);
      db.dropBucket(bucket);
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void replicationClean(String sourceHost, String sourceUser, String sourcePassword,
                                       boolean sourceSsl, String sourceBucket,
                                       String targetHost, String targetUser, String targetPassword,
                                       boolean targetSsl, String targetBucket) {
    CouchbaseXDCR.XDCRBuilder xdcrBuilder = new CouchbaseXDCR.XDCRBuilder();
    CouchbaseConnect.CouchbaseBuilder sourceBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect.CouchbaseBuilder targetBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect sourceDb;
    CouchbaseConnect targetDb;

    try {
      sourceBuilder.connect(sourceHost, sourceUser, sourcePassword)
          .ssl(sourceSsl)
          .bucket(sourceBucket);
      sourceDb = sourceBuilder.build();
      targetBuilder.connect(targetHost, targetUser, targetPassword)
          .ssl(targetSsl)
          .bucket(targetBucket);
      targetDb = targetBuilder.build();

      System.err.printf("Removing replicating %s:%s -> %s:%s\n", sourceHost, sourceBucket, targetHost, targetBucket);
      CouchbaseXDCR xdcr = xdcrBuilder.source(sourceDb).target(targetDb).build();
      xdcr.removeReplication();

      sourceDb.disconnect();
      targetDb.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
