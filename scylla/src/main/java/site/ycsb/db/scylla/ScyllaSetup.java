package site.ycsb.db.scylla;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ScyllaSetup {

  public static final String KEYSPACE_PROPERTY = "scylla.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "scylla.username";
  public static final String PASSWORD_PROPERTY = "scylla.password";

  public static final String HOSTS_PROPERTY = "scylla.hosts";
  public static final String PORT_PROPERTY = "scylla.port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String USE_SSL_CONNECTION = "scylla.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  public static final String FIELD_COUNT_PROPERTY = "fieldcount";
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  public static final String TABLE_NAME_PROPERTY = "scylla.tablename";
  public static final String TABLE_NAME_PROPERTY_DEFAULT = "usertable";

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

    int fieldcount = Integer.parseInt(properties.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    String tablename = properties.getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);

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

    System.out.printf("Creating keyspace %s%n", keyspace);

    StringBuilder sql = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ");
    sql.append(keyspace);
    sql.append(" WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3}");
    sql.append(" AND durable_writes = false");

    session.execute(sql.toString());
    session.close();
    session = cluster.connect(keyspace);

    System.out.println("Creating table usertable");

    sql.setLength(0);
    sql.append("create table if not exists ");
    sql.append(tablename);
    sql.append(" (y_id varchar primary key");

    for (int idx = 0; idx < fieldcount; idx++) {
      sql.append(", field");
      sql.append(idx);
      sql.append(" varchar");
    }
    sql.append(")");

    session.execute(sql.toString());
    session.close();
    cluster.close();
  }

  private ScyllaSetup() {
    super();
  }
}
