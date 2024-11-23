package site.ycsb.db.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import org.apache.commons.cli.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

public class CassandraSetup {
  public static final String YCSB_KEY = "y_id";

  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String HOSTS_PROPERTY = "cassandra.hosts";
  public static final String PORT_PROPERTY = "cassandra.port";
  public static final String DATACENTER_PROPERTY = "cassandra.datacenter";
  public static final String PORT_PROPERTY_DEFAULT = "9042";
  public static final String DATACENTER_PROPERTY_DEFAULT = "dc1";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  public static final String FIELD_COUNT_PROPERTY = "fieldcount";
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  public static final String TABLE_NAME_PROPERTY = "cassandra.tablename";
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
      createKeyspace(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void createKeyspace(Properties properties) throws Exception {
    boolean useSSL = Boolean.parseBoolean(
        properties.getProperty(USE_SSL_CONNECTION, DEFAULT_USE_SSL_CONNECTION));
    String host = properties.getProperty(HOSTS_PROPERTY);
    String port = properties.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);
    if (host == null) {
      throw new RuntimeException(String.format("Required property \"%s\" missing for Cassandra Client", HOSTS_PROPERTY));
    }
    String[] hosts = host.split(",");
    Collection<InetSocketAddress> hostList = new LinkedList<>();
    for (String name : hosts) {
      hostList.add(new InetSocketAddress(name, Integer.parseInt(port)));
    }

    String username = properties.getProperty(USERNAME_PROPERTY);
    String password = properties.getProperty(PASSWORD_PROPERTY);

    String keyspace = properties.getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

    int fieldcount = Integer.parseInt(properties.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    String tablename = properties.getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);

    String datacenter = properties.getProperty(DATACENTER_PROPERTY, DATACENTER_PROPERTY_DEFAULT);

    CqlSessionBuilder builder = CqlSession.builder();
    if ((username != null) && !username.isEmpty()) {
      builder = builder
          .withAuthCredentials(username, password)
          .addContactPoints(hostList);
      if (useSSL) {
        TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
              public void checkClientTrusted(X509Certificate[] chain, String authType) {}
              public void checkServerTrusted(X509Certificate[] chain, String authType) {}
              public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        ProgrammaticSslEngineFactory sslEngineFactory = new ProgrammaticSslEngineFactory(sslContext);

        builder = builder.withSslEngineFactory(sslEngineFactory);
      }
    } else {
      builder = builder.addContactPoints(hostList);
    }
    builder.withLocalDatacenter(datacenter);

    ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder();
    loaderBuilder.withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, Duration.ofMillis(5000));
    loaderBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(10000));
    loaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(10000));

    DriverConfigLoader loader = loaderBuilder.build();
    builder.withConfigLoader(loader);

    CqlSession session = builder.build();

    Thread.sleep(1000);
    System.out.printf("Creating keyspace %s%n", keyspace);

    CreateKeyspace createKs = SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(1);
    session.execute(createKs.build());

    session.close();
    builder.withKeyspace(keyspace);
    session = builder.build();

    System.out.printf("Creating table %s%n", tablename);

    CreateTable createTable = createTable(keyspace, tablename).ifNotExists().withPartitionKey(YCSB_KEY, DataTypes.TEXT);

    for (int idx = 0; idx < fieldcount; idx++) {
      createTable = createTable.withColumn(String.format("field%d", idx), DataTypes.TEXT);
    }

    session.execute(createTable.build());
    session.close();
  }

  private CassandraSetup() {
    super();
  }
}
