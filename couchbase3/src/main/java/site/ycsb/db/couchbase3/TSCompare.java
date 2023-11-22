package site.ycsb.db.couchbase3;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.GetResult;
import org.apache.commons.cli.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Compare Timestamps.
 */
public final class TSCompare {
  public static final String SOURCE_HOST = "source.hostname";
  public static final String SOURCE_USER = "source.user";
  public static final String SOURCE_PASSWORD = "source.password";
  public static final String SOURCE_BUCKET = "source.bucket";

  private static void checkTimestamps(Properties properties) throws RuntimeException {
    CouchbaseConnect db;
    String sourceHost = properties.getProperty(SOURCE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String sourceUser = properties.getProperty(SOURCE_USER, CouchbaseConnect.DEFAULT_USER);
    String sourcePassword = properties.getProperty(SOURCE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    String sourceBucket = properties.getProperty(SOURCE_BUCKET, "ycsb");

    System.out.println("Host: " + sourceHost);
    System.out.println("User: " + sourceUser);

    db = CouchbaseConnect.getInstance();

    db.connect(sourceHost, sourceUser, sourcePassword, CouchbaseConnect.ConnectMode.PRIMARY);

    Collection collection = db.keyspace(sourceBucket, CouchbaseConnect.ConnectMode.PRIMARY);
    GetResult result = collection.get("usertable::user1000053778378872380");
    System.out.println(result.toString());
    db.getDocs(sourceBucket, CouchbaseConnect.ConnectMode.PRIMARY);
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
      checkTimestamps(properties);
    } catch (RuntimeException e) {
      System.err.println("Error: " + e);
      System.exit(1);
    }
  }

  private TSCompare() {
    super();
  }
}
