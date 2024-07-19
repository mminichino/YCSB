package site.ycsb.db.couchbase3;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Clean Cluster after Testing.
 */
public class ColumnarS3Load {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_SCOPE = "couchbase.scope";
  public static final String CLUSTER_S3_BUCKET = "couchbase.s3Bucket";
  public static final String CLUSTER_DB_LINK = "couchbase.dbLink";
  public static final String COLUMNAR_IMPORT_TYPE = "columnar.importType";

  public enum ImportFileType {
    JSON,
    PARQUET
  }

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "source properties");
    Option collectionOpt = new Option("c", "collection", true, "source collection");
    Option showOpt = new Option("s", "show", false, "show SQL");
    source.setRequired(true);
    collectionOpt.setRequired(false);
    showOpt.setRequired(false);
    options.addOption(source);
    options.addOption(collectionOpt);
    options.addOption(showOpt);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("ClusterInit", options);
      System.exit(1);
    }

    String propFile = cmd.getOptionValue("properties");

    try {
      properties.load(Files.newInputStream(Paths.get(propFile)));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }

    String collectionName = cmd.hasOption("collection") ? cmd.getOptionValue("collection") : null;
    boolean showSQL = cmd.hasOption("show");

    try {
      doImport(properties, collectionName, showSQL);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void doImport(Properties properties, String collectionName, boolean showSQL) {
    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "bench");
    String clusterScope = properties.getProperty(CLUSTER_SCOPE, "bench");
    String s3Bucket = properties.getProperty(CLUSTER_S3_BUCKET, "bucket");
    String dbLink = properties.getProperty(CLUSTER_DB_LINK, "data_link");
    ImportFileType importType = ImportFileType.valueOf(properties.getProperty(COLUMNAR_IMPORT_TYPE, "JSON").toUpperCase());

    System.err.println("Starting S3 load");

    columnarSetup(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket, clusterScope, s3Bucket, dbLink, importType, collectionName, showSQL);
  }

  public static void createAnalyticsCollection(CouchbaseConnect db, String bucketName, String scopeName, String name, String[] keys) {
    String statement;
    List<String> pkBlock = new ArrayList<>();

    for (String key : keys) {
      pkBlock.add(key + ":int");
    }

    statement = "CREATE DATABASE " + bucketName + " IF NOT EXISTS";
    db.analyticsQuery(statement);
    statement = "CREATE SCOPE " + bucketName + "." + scopeName + " IF NOT EXISTS";
    db.analyticsQuery(statement);
    statement = "CREATE COLLECTION " + bucketName + "." + scopeName + "." + name + " IF NOT EXISTS PRIMARY KEY (" + String.join(",", pkBlock) + ")";
    db.analyticsQuery(statement);
  }

  public static boolean loadAnalyticsCollection(CouchbaseConnect db, String statement, String table, String s3Bucket, String dbLink, boolean showSQL) {
    String copyIntoSQL = String.format(statement, table, s3Bucket, dbLink, table);
    if (showSQL) {
      System.out.println(copyIntoSQL);
      return true;
    } else {
      List<ObjectNode> result = db.analyticsScopeQuery(copyIntoSQL);
      return result != null;
    }
  }

  public static boolean optimizeAnalyticsCollection(CouchbaseConnect db, String bucketName, String scopeName, String name, boolean showSQL) {
    String optimize = "ANALYZE COLLECTION " + bucketName + "." + scopeName + "." + name + " WITH {\"sample\": \"high\"};";
    if (showSQL) {
      System.out.println(optimize);
      return true;
    } else {
      List<ObjectNode> result = db.analyticsScopeQuery(optimize);
      return result != null;
    }
  }

  private static void columnarSetup(String host, String user, String password, boolean ssl, String bucket, String scope,
                                    String s3Bucket, String dbLink, ImportFileType importType, String collectionName,
                                    boolean showSQL) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;
    String statement;
    String[] tableNames = {"item", "warehouse", "stock", "district", "customer", "history", "orders", "new_orders", "order_line", "supplier", "nation", "region"};
    String[][] keyNames = {
        {"i_id"},
        {"w_id"},
        {"s_i_id", "s_w_id"},
        {"d_id", "d_w_id"},
        {"c_id", "c_d_id", "c_w_id"},
        {"h_c_id", "h_c_d_id", "h_c_w_id"},
        {"o_id", "o_d_id", "o_w_id"},
        {"no_o_id", "no_d_id", "no_w_id"},
        {"ol_o_id", "ol_d_id", "ol_w_id", "ol_number"},
        {"su_suppkey"},
        {"n_nationkey"},
        {"r_regionkey"}
    };
    if (importType == ImportFileType.JSON) {
      statement = "COPY INTO `%s` FROM `%s` AT `%s` PATH '%s' WITH { 'format': 'json', 'include': ['*.gzip', '*.gz'] }";
    } else {
      statement = "COPY INTO `%s` FROM `%s` AT `%s` PATH '%s' WITH { 'format': 'parquet', 'include': ['*.parquet', '*.pqt'] }";
    }

    try {
      dbBuilder.connect(host, user, password)
          .ssl(ssl)
          .columnar(true)
          .bucket(bucket)
          .scope(scope);
      db = dbBuilder.build();
      for (int i = 0; i < tableNames.length; i++) {
        String table = tableNames[i];
        String[] keys = keyNames[i];
        if (collectionName != null && !collectionName.equals(table)) {
          continue;
        }
        System.out.printf("Importing table %s (primary keys: %s)\n", table, String.join(",", keys));
        createAnalyticsCollection(db, bucket, scope, table, keys);
        if (!loadAnalyticsCollection(db, statement, table, s3Bucket, dbLink, showSQL)) {
          System.err.printf("Error: table %s import failed\n", table);
        }
        if (!optimizeAnalyticsCollection(db, bucket, scope, table, showSQL)) {
          System.err.printf("Error: table %s analyze failed\n", table);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ColumnarS3Load() {
    super();
  }
}
