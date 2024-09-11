package site.ycsb.db.scylla;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ScyllaCleanup {

  public static final String KEYSPACE_PROPERTY = "scylla.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "scylla.username";
  public static final String PASSWORD_PROPERTY = "scylla.password";

  public static final String HOSTS_PROPERTY = "scylla.hosts";
  public static final String PORT_PROPERTY = "scylla.port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String USE_SSL_CONNECTION = "scylla.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

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
      formatter.printHelp("CreateDatabase", options);
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

    try {
      createCollection(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void createCollection(Properties properties) {
    boolean useSSL = Boolean.parseBoolean(
        properties.getProperty(USE_SSL_CONNECTION, DEFAULT_USE_SSL_CONNECTION));
    String host = properties.getProperty(HOSTS_PROPERTY);
    if (host == null) {
      throw new RuntimeException(String.format("Required property \"%s\" missing for scyllaCQLClient", HOSTS_PROPERTY));
    }
    String[] hosts = host.split(",");
    String port = properties.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

    String username = properties.getProperty(USERNAME_PROPERTY);
    String password = properties.getProperty(PASSWORD_PROPERTY);

    String keyspace = properties.getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

    Cluster.Builder builder;
    if ((username != null) && !username.isEmpty()) {
      builder = Cluster.builder().withCredentials(username, password)
          .addContactPoints(hosts).withPort(Integer.parseInt(port));
      if (useSSL) {
        builder = builder.withSSL();
      }
    } else {
      builder = Cluster.builder().withPort(Integer.parseInt(port))
          .addContactPoints(hosts);
    }

    Cluster cluster = builder.build();
    Session session = cluster.connect();

    System.out.printf("Dropping keyspace %s%n", keyspace);

    session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
    session.close();
    cluster.close();
  }

  private ScyllaCleanup() {
    super();
  }
}
