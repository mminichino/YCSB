package site.ycsb.db.couchbase3;

import site.ycsb.TableKeyType;
import site.ycsb.TableKeys;

import java.util.Arrays;
import java.util.Properties;

/**
 * Clean Cluster after Testing.
 */
public class ColumnarS3Export {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_SCOPE = "couchbase.scope";
  public static final String CLUSTER_S3_BUCKET = "couchbase.s3Bucket";
  public static final String CLUSTER_DB_LINK = "couchbase.dbLink";

  public void configure(Properties properties) {
    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "bench");
    String clusterScope = properties.getProperty(CLUSTER_SCOPE, "bench");
    String s3Bucket = properties.getProperty(CLUSTER_S3_BUCKET, "bucket");
    String dbLink = properties.getProperty(CLUSTER_DB_LINK, "data_link");

    System.err.println("Starting S3 load");

    columnarSetup(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket, clusterScope, s3Bucket, dbLink);
  }

  public void createAnalyticsCollection(CouchbaseConnect db, String bucketName, String scopeName, String name, TableKeys keys) {
    String statement;
    String keyType;

    switch(keys.primaryKeyType) {
      case INTEGER:
        keyType = "int";
        break;
      case FLOAT:
        keyType = "double";
        break;
      default:
        keyType = "string";
    }

    statement = "CREATE DATABASE " + bucketName + " IF NOT EXISTS";
    db.analyticsQuery(statement);
    statement = "CREATE SCOPE " + bucketName + "." + scopeName + " IF NOT EXISTS";
    db.analyticsQuery(statement);
    statement = "CREATE COLLECTION " + bucketName + "." + scopeName + "." + name + " IF NOT EXISTS PRIMARY KEY (" + keys.primaryKeyName + ":" + keyType + ")";
    db.analyticsQuery(statement);
  }

  private void columnarSetup(String host, String user, String password, boolean ssl, String bucket, String scope, String s3Bucket, String dbLink) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;
    String[] tableNames = {"item", "warehouse", "stock", "district", "customer", "history", "orders", "new_orders", "order_line", "supplier", "nation", "region"};
    String[] keyNames = {"i_id", "w_id", "s_i_id", "d_id", "c_id", "h_c_id", "o_id", "no_o_id", "ol_o_id", "su_suppkey", "n_nationkey", "r_regionkey"};
    String statement = "COPY %s TO `%s` AT `%s` PATH(\"%s\") WITH {'compression': 'gzip'}";

    try {
      dbBuilder.connect(host, user, password)
          .ssl(ssl)
          .bucket(bucket)
          .scope(scope);
      db = dbBuilder.build();
      for (int i = 0; i < tableNames.length; i++) {
        String table = tableNames[i];
        String key = keyNames[i];
        createAnalyticsCollection(db, bucket, scope, table, new TableKeys().create(key, TableKeyType.INTEGER));
        db.analyticsScopeQuery(statement, Arrays.asList(table, s3Bucket, dbLink, table));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
