package site.ycsb.db.couchbase3;

import site.ycsb.TestSetup;

import java.util.Properties;

/**
 * Prepare Cluster for Testing.
 */
public class CouchbaseQuerySetup extends TestSetup {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_SCOPE = "couchbase.scope";
  public static final String CLUSTER_COLLECTION = "couchbase.collection";
  public static final String CLUSTER_PROJECT = "couchbase.project";
  public static final String CLUSTER_DATABASE = "couchbase.database";
  public static final String CLUSTER_BUCKET_TYPE = "couchbase.bucketType";
  public static final String CLUSTER_REPLICA_NUM = "couchbase.replicaNum";
  public static final String INDEX_CREATE = "index.create";
  public static final String INDEX_FIELD = "index.field";

  @Override
  public void testSetup(Properties properties) {
    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "ycsb");
    String clusterScope = properties.getProperty(CLUSTER_SCOPE, "_default");
    String clusterCollection = properties.getProperty(CLUSTER_COLLECTION, "usertable");
    String clusterProject = properties.getProperty(CLUSTER_PROJECT, null);
    String clusterDatabase = properties.getProperty(CLUSTER_DATABASE, null);
    int clusterBucketType = Integer.parseInt(properties.getProperty(CLUSTER_BUCKET_TYPE, "0"));
    int clusterReplicaNum = Integer.parseInt(properties.getProperty(CLUSTER_REPLICA_NUM, "1"));

    boolean indexCreate = properties.getProperty(INDEX_CREATE, "false").equals("true");
    String indexField = properties.getProperty(INDEX_FIELD, "meta().id");

    System.err.println("Starting test setup");

    clusterSetup(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket, clusterScope, clusterCollection,
        clusterBucketType, clusterReplicaNum, clusterProject, clusterDatabase, indexCreate, indexField);
  }

  private static void clusterSetup(String host, String user, String password, boolean ssl, String bucket, String scope,
                                   String collection, int type, int replicas, String project, String database,
                                   boolean index, String field) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;
    String typeText = type == 1 ? "Magma" : "Couchstore";

    try {
      dbBuilder.connect(host, user, password)
          .ssl(ssl)
          .bucket(bucket);
      if (project != null && database != null) {
        dbBuilder.capella(project, database);
      }
      db = dbBuilder.build();
      System.err.printf("Creating bucket %s (%s) on cluster:[%s]\n", bucket, typeText, host);
      db.createBucket(bucket, replicas, type);
      System.err.printf("Creating collection %s\n", collection);
      db.createCollection(bucket, scope, collection);
      if (db.isAnalyticsEnabled()) {
        System.err.printf("Creating analytics collection %s\n", bucket);
        db.createAnalyticsCollection(bucket);
      }
      if (index) {
        System.err.printf("Creating index on field %s\n", field);
        db.createFieldIndex(field);
      }
      db.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
