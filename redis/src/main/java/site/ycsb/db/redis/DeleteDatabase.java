package site.ycsb.db.redis;

import org.apache.commons.cli.*;
import site.ycsb.REST;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Prepare Cluster for Testing.
 */
public final class DeleteDatabase {

  public static final String HOST_PROPERTY = "redis.endpoints";
  public static final String USERNAME_PROPERTY = "redis.username";
  public static final String PASSWORD_PROPERTY = "redis.password";

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

    try {
      deleteDatabase(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void deleteDatabase(Properties properties) {
    String endpoints = properties.getProperty(HOST_PROPERTY);
    String[] endpointsArray = endpoints.split(",");
    String[] host = endpointsArray[0].split(":");
    String hostname = host[0];
    String username = properties.getProperty(USERNAME_PROPERTY);
    String password = properties.getProperty(PASSWORD_PROPERTY);

    System.err.printf("Deleting database on %s as user %s", hostname, username);

    String endpoint = "/v1/bdbs/1";
    REST client = new REST(hostname, username, password, true, 9443);

    client.delete(endpoint).validate();
  }

  private DeleteDatabase() {
    super();
  }

}
