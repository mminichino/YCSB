package site.ycsb.db.couchbase3;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Prepare Cluster for Testing.
 */
public final class ClusterPrep {
  public static final String SOURCE_HOST = "couchbase.hostname";
  public static final String SOURCE_USER = "couchbase.username";
  public static final String SOURCE_PASSWORD = "couchbase.password";
  public static final String SOURCE_BUCKET = "couchbase.bucket";
  public static final String SOURCE_PROJECT = "couchbase.project";
  public static final String SOURCE_DATABASE = "couchbase.database";
  public static final String SOURCE_EVENTING = "couchbase.eventing";
  public static final String TARGET_HOST = "xdcr.hostname";
  public static final String TARGET_USER = "xdcr.username";
  public static final String TARGET_PASSWORD = "xdcr.password";
  public static final String TARGET_BUCKET = "xdcr.bucket";
  public static final String TARGET_PROJECT = "xdcr.project";
  public static final String TARGET_DATABASE = "xdcr.database";
  public static final String TARGET_EVENTING = "xdcr.eventing";
  public static final String TARGET_EXTERNAL = "xdcr.external";

  private static void prepCluster(Properties properties, Boolean removeFlag) {
    String sourceHost = properties.getProperty(SOURCE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String sourceUser = properties.getProperty(SOURCE_USER, CouchbaseConnect.DEFAULT_USER);
    String sourcePassword = properties.getProperty(SOURCE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String sourceBucket = properties.getProperty(SOURCE_BUCKET, "ycsb");
    String sourceProject = properties.getProperty(SOURCE_PROJECT, null);
    String sourceDatabase = properties.getProperty(SOURCE_DATABASE, null);
    String sourceEventing = properties.getProperty(SOURCE_EVENTING, null);

    String targetHost = properties.getProperty(TARGET_HOST, null);
    String targetUser = properties.getProperty(TARGET_USER, CouchbaseConnect.DEFAULT_USER);
    String targetPassword = properties.getProperty(TARGET_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String targetBucket = properties.getProperty(TARGET_BUCKET, "ycsb");
    String targetProject = properties.getProperty(TARGET_PROJECT, null);
    String targetDatabase = properties.getProperty(TARGET_DATABASE, null);
    String targetEventing = properties.getProperty(TARGET_EVENTING, null);
    Boolean targetExternal = properties.getProperty(TARGET_EXTERNAL, "false").equals("true");

    if (!removeFlag) {
      System.out.println("Preparing cluster " + sourceHost);
      doPrep(sourceHost, sourceUser, sourcePassword, sourceBucket, sourceProject, sourceDatabase, sourceEventing);

      if (targetHost != null) {
        System.out.println("Preparing XDCR target cluster " + targetHost);
        doPrep(targetHost, targetUser, targetPassword, targetBucket, targetProject, targetDatabase, targetEventing);
        System.out.println("Configuring replication");
        doReplicate(sourceHost, sourceBucket, sourceUser, sourcePassword,
            targetHost, targetBucket, targetUser, targetPassword, targetExternal);
      }
    } else {
      if (targetHost != null) {
        System.out.println("Removing replication");
        doDropReplication(sourceHost, sourceBucket, sourceUser, sourcePassword, targetHost, targetBucket);
        System.out.println("Cleaning XDCR target cluster " + targetHost);
        doDrop(targetHost, targetUser, targetPassword, targetBucket, targetProject, targetDatabase, targetEventing);
      }

      System.out.println("Cleaning cluster " + sourceHost);
      doDrop(sourceHost, sourceUser, sourcePassword, sourceBucket, sourceProject, sourceDatabase, sourceEventing);
    }
  }

  private static void doPrep(String hostname, String username, String password, String bucket,
                             String project, String database, String function) {
    CouchbaseConnect db;
    try {
      if (project == null) {
        db = new CouchbaseConnect(hostname, username, password);
      } else {
        db = new CouchbaseConnect(hostname, username, password, project, database);
      }
    } catch (CouchbaseConnectException e) {
      throw new RuntimeException(e);
    }

    if (function != null) {
      db.bucketCreate("eventing", 128, 1);
    }

    db.bucketCreate(bucket, 1);
    try {
      db.keyspace(bucket);
    } catch (CouchbaseConnectException e) {
      throw new RuntimeException(e);
    }

    if (function != null) {
      try {
        db.deployEventingFunction(function, "eventing");
      } catch (CouchbaseConnectException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static void doReplicate(String sourceHost, String sourceBucket, String sourceUser, String sourcePassword,
                                  String targetHost, String targetBucket, String targetUser, String targetPassword,
                                  Boolean external) {
    CouchbaseConnect db;
    try {
      db = new CouchbaseConnect(sourceHost, sourceUser, sourcePassword);
    } catch (CouchbaseConnectException e) {
      throw new RuntimeException(e);
    }

    db.createXDCRReference(targetHost, targetUser, targetPassword, external);
    db.createXDCRReplication(targetHost, sourceBucket, targetBucket);
  }

  private static void doDropReplication(String sourceHost, String sourceBucket, String sourceUser, String sourcePassword,
                                        String targetHost, String targetBucket) {
    CouchbaseConnect db;
    try {
      db = new CouchbaseConnect(sourceHost, sourceUser, sourcePassword);
    } catch (CouchbaseConnectException e) {
      throw new RuntimeException(e);
    }

    db.deleteXDCRReplication(targetHost, sourceBucket, targetBucket);
    db.deleteXDCRReference(targetHost);
  }

  private static void doDrop(String hostname, String username, String password, String bucket,
                             String project, String database, String function) {
    CouchbaseConnect db;
    try {
      if (project == null) {
        db = new CouchbaseConnect(hostname, username, password);
      } else {
        db = new CouchbaseConnect(hostname, username, password, project, database);
      }
    } catch (CouchbaseConnectException e) {
      throw new RuntimeException(e);
    }

    if (function != null) {
      db.dropBucket("eventing");
    }

    db.dropBucket(bucket);
  }

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "source properties");
    source.setRequired(true);
    options.addOption(source);

    Option remove = new Option("R", "remove", false, "Remove assets");
    remove.setRequired(false);
    options.addOption(remove);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("TSCompare", options);
      System.exit(1);
    }

    String propFile = cmd.getOptionValue("properties");

    try {
      properties.load(Files.newInputStream(Paths.get(propFile)));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      System.exit(1);
    }

    try {
      prepCluster(properties, cmd.hasOption("remove"));
    } catch (Exception e) {
      System.err.println("Error: " + e);
      System.exit(1);
    }
  }

  private ClusterPrep() {
    super();
  }
}
