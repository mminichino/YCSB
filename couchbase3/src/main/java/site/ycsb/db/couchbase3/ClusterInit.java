package site.ycsb.db.couchbase3;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Prepare Cluster for Testing.
 */
public final class ClusterInit {
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

  private static void prepCluster(Properties properties, boolean onlySource) {
    CouchbaseConnect.CouchbaseBuilder sourceBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect.CouchbaseBuilder targetBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseXDCR.XDCRBuilder xdcrBuilder = new CouchbaseXDCR.XDCRBuilder();
    CouchbaseConnect sourceDb;
    CouchbaseConnect targetDb;

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

    System.out.println("Starting cluster init");
    System.out.printf("Cluster: %s\n", sourceHost);

    try {
      sourceBuilder.connect(sourceHost, sourceUser, sourcePassword)
          .bucket(sourceBucket);
      if (sourceProject != null && sourceDatabase != null) {
        sourceBuilder.capella(sourceProject, sourceDatabase);
      }
      sourceDb = sourceBuilder.build();
      if (sourceEventing != null) {
        System.out.printf("Creating eventing bucket [%s]\n", sourceHost);
        sourceDb.createBucket("eventing", 128, 1);
      }
      System.out.printf("Creating %s bucket [%s]\n", sourceBucket, sourceHost);
      sourceDb.createBucket(sourceBucket, 1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (targetHost != null && !onlySource) {
      System.out.printf("Remote : %s\n", targetHost);
      try {
        targetBuilder.connect(targetHost, targetUser, targetPassword)
            .bucket(targetBucket);
        if (targetProject != null && targetDatabase != null) {
          sourceBuilder.capella(targetProject, targetDatabase);
        }
        targetDb = targetBuilder.build();
        if (targetEventing != null) {
          System.out.printf("Creating target eventing bucket [%s]\n", targetHost);
          targetDb.createBucket("eventing", 128, 1);
        }
        System.out.printf("Creating target %s bucket [%s]\n", targetBucket, targetHost);
        targetDb.createBucket(targetBucket, 1);

        System.out.printf("Replicating %s -> %s\n", sourceBucket, targetBucket);
        CouchbaseXDCR xdcr = xdcrBuilder.source(sourceDb).target(targetDb).build();
        xdcr.createReplication();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "source properties");
    source.setRequired(true);
    options.addOption(source);

    Option remove = new Option("S", "source", false, "Only config source");
    remove.setRequired(false);
    options.addOption(remove);

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
      System.exit(1);
    }

    try {
      prepCluster(properties, cmd.hasOption("source"));
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private ClusterInit() {
    super();
  }
}
