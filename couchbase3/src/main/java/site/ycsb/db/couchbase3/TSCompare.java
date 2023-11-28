package site.ycsb.db.couchbase3;

import com.couchbase.client.java.Collection;
import com.couchbase.client.core.deps.com.google.gson.JsonParser;
import com.couchbase.client.core.deps.com.google.gson.JsonObject;

import org.apache.commons.cli.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Compare Timestamps.
 */
public final class TSCompare {
  public static final String SOURCE_HOST = "source.hostname";
  public static final String SOURCE_USER = "source.user";
  public static final String SOURCE_PASSWORD = "source.password";
  public static final String SOURCE_BUCKET = "source.bucket";
  public static final String TARGET_HOST = "target.hostname";
  public static final String TARGET_USER = "target.user";
  public static final String TARGET_PASSWORD = "target.password";
  public static final String TARGET_BUCKET = "target.bucket";

  private static void checkTimestamps(Properties properties) throws RuntimeException, InterruptedException {
    CouchbaseConnect db;
    String sourceHost = properties.getProperty(SOURCE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String sourceUser = properties.getProperty(SOURCE_USER, CouchbaseConnect.DEFAULT_USER);
    String sourcePassword = properties.getProperty(SOURCE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String sourceBucket = properties.getProperty(SOURCE_BUCKET, "ycsb");

    String targetHost = properties.getProperty(TARGET_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String targetUser = properties.getProperty(TARGET_USER, CouchbaseConnect.DEFAULT_USER);
    String targetPassword = properties.getProperty(TARGET_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String targetBucket = properties.getProperty(TARGET_BUCKET, "ycsb");

    Map<String, Long> sourceTimeMap = new HashMap<>();
    Map<String, Long> targetTimeMap = new HashMap<>();

    System.out.println("Host: " + sourceHost);
    System.out.println("User: " + sourceUser);

    db = CouchbaseConnect.getInstance();

    db.connect(targetHost, targetUser, targetPassword);

    Collection collection = db.keyspace(sourceBucket);

    AtomicLong sourceDocCount = new AtomicLong(0);
    AtomicLong targetDocCount = new AtomicLong(0);

    Thread sourceThread = new Thread(() -> {
      System.out.print("Starting source listener\n");
      CouchbaseStream s_dcp = new CouchbaseStream(sourceHost, sourceUser, sourcePassword, sourceBucket, true);
      s_dcp.streamDocuments();
      s_dcp.startToNow();
      s_dcp.getDrain().forEach(item -> {
        JsonObject dataObject = JsonParser.parseString(item).getAsJsonObject();
        JsonObject metadata = dataObject.get("meta").getAsJsonObject();
        JsonObject document = dataObject.get("document").getAsJsonObject();
        String id = metadata.get("id").getAsString();
        long sourceTime = document.get("timestamp").getAsLong();
        sourceTimeMap.put(id, sourceTime);
        System.out.println("Source => " + id + " s_timestamp: " + sourceTime);
      });
      s_dcp.stop();
      sourceDocCount.set(s_dcp.getCount());
    });

    Thread targetThread = new Thread(() -> {
      System.out.print("Starting target listener\n");
      CouchbaseStream t_dcp = new CouchbaseStream(targetHost, targetUser, targetPassword, targetBucket, true);
      t_dcp.streamDocuments();
      t_dcp.startToNow();
      t_dcp.getDrain().forEach(item -> {
        JsonObject dataObject = JsonParser.parseString(item).getAsJsonObject();
        JsonObject metadata = dataObject.get("meta").getAsJsonObject();
        JsonObject document = dataObject.get("document").getAsJsonObject();
        String id = metadata.get("id").getAsString();
        long targetTime = document.get("timestamp").getAsLong();
        targetTimeMap.put(id, targetTime);
        System.out.println("Target => " + id + " s_timestamp: " + targetTime);
      });
      t_dcp.stop();
      targetDocCount.set(t_dcp.getCount());
    });

    sourceThread.start();
    targetThread.start();
    sourceThread.join();
    targetThread.join();

    long totalCount = 0;
    long totalTime = 0;
    long maxTime = 0;
    double average;
    for (Map.Entry<String, Long> entry : sourceTimeMap.entrySet()) {
      String id = entry.getKey();
      long sourceTime = entry.getValue();
      long targetTime = targetTimeMap.get(id);
      long timeDiff = targetTime - sourceTime;
      totalTime += timeDiff;
      if (timeDiff > maxTime) maxTime = timeDiff;
      totalCount += 1;
      System.out.printf("Compare => %s :: %d, %d diff %d\n", id, entry.getValue(), targetTimeMap.get(id), timeDiff);
    }

    average = (double) totalTime / totalCount;
    System.out.println(sourceDocCount);
    System.out.println(targetDocCount);
    System.out.printf("Average: %.02f\n", average);
    System.out.printf("Maximum: %d\n", maxTime);
  }

  private static void listenTimestamps(Properties properties) throws RuntimeException, InterruptedException {
    CouchbaseConnect db;
    String sourceHost = properties.getProperty(SOURCE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String sourceUser = properties.getProperty(SOURCE_USER, CouchbaseConnect.DEFAULT_USER);
    String sourcePassword = properties.getProperty(SOURCE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String sourceBucket = properties.getProperty(SOURCE_BUCKET, "ycsb");

    String targetHost = properties.getProperty(TARGET_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String targetUser = properties.getProperty(TARGET_USER, CouchbaseConnect.DEFAULT_USER);
    String targetPassword = properties.getProperty(TARGET_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String targetBucket = properties.getProperty(TARGET_BUCKET, "ycsb");

    Map<String, Long> sourceTimeMap = new HashMap<>();
    Map<String, Long> targetTimeMap = new HashMap<>();

    System.out.println("Host: " + sourceHost);
    System.out.println("User: " + sourceUser);

    db = CouchbaseConnect.getInstance();

    db.connect(targetHost, targetUser, targetPassword);

    Collection collection = db.keyspace(sourceBucket);

    AtomicLong sourceDocCount = new AtomicLong(0);
    AtomicLong targetDocCount = new AtomicLong(0);

    Thread sourceThread = new Thread(() -> {
      System.out.print("Starting source listener\n");
      CouchbaseStream s_dcp = new CouchbaseStream(sourceHost, sourceUser, sourcePassword, sourceBucket, true);
      s_dcp.streamDocuments();
      s_dcp.startFromNow();
      s_dcp.getByCount(100_000).forEach(item -> {
        JsonObject dataObject = JsonParser.parseString(item).getAsJsonObject();
        JsonObject metadata = dataObject.get("meta").getAsJsonObject();
//        JsonObject document = dataObject.get("document").getAsJsonObject();
        String id = metadata.get("id").getAsString();
        long sourceTime = metadata.get("timestamp").getAsLong();
        sourceTimeMap.put(id, sourceTime);
        System.out.println("Source => " + id + " s_timestamp: " + sourceTime);
      });
      s_dcp.stop();
      sourceDocCount.set(s_dcp.getCount());
    });

    Thread targetThread = new Thread(() -> {
      System.out.print("Starting target listener\n");
      CouchbaseStream t_dcp = new CouchbaseStream(targetHost, targetUser, targetPassword, targetBucket, true);
      t_dcp.streamDocuments();
      t_dcp.startFromNow();
      t_dcp.getByCount(100_000).forEach(item -> {
        JsonObject dataObject = JsonParser.parseString(item).getAsJsonObject();
        JsonObject metadata = dataObject.get("meta").getAsJsonObject();
//        JsonObject document = dataObject.get("document").getAsJsonObject();
        String id = metadata.get("id").getAsString();
        long targetTime = metadata.get("timestamp").getAsLong();
        targetTimeMap.put(id, targetTime);
        System.out.println("Target => " + id + " s_timestamp: " + targetTime);
      });
      t_dcp.stop();
      targetDocCount.set(t_dcp.getCount());
    });

    sourceThread.start();
    targetThread.start();
    sourceThread.join();
    targetThread.join();

    long totalCount = 0;
    long totalTime = 0;
    long maxTime = 0;
    double average;
    for (Map.Entry<String, Long> entry : sourceTimeMap.entrySet()) {
      String id = entry.getKey();
      long sourceTime = entry.getValue();
      long targetTime = targetTimeMap.get(id);
      long timeDiff = targetTime - sourceTime;
      totalTime += timeDiff;
      if (timeDiff > maxTime) maxTime = timeDiff;
      totalCount += 1;
      System.out.printf("Compare => %s :: %d, %d diff %d\n", id, entry.getValue(), targetTimeMap.get(id), timeDiff);
    }

    average = (double) totalTime / totalCount;
    System.out.println(sourceDocCount);
    System.out.println(targetDocCount);
    System.out.printf("Average: %.02f\n", average);
    System.out.printf("Maximum: %d\n", maxTime);
  }

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "source properties");
    source.setRequired(true);
    options.addOption(source);

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
      System.out.println("Starting");
      listenTimestamps(properties);
    } catch (RuntimeException | InterruptedException e) {
      System.err.println("Error: " + e);
      System.exit(1);
    }
  }

  private TSCompare() {
    super();
  }
}
