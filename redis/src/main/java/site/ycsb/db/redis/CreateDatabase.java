package site.ycsb.db.redis;

import com.google.gson.JsonArray;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import com.google.gson.JsonObject;
import site.ycsb.REST;

/**
 * Prepare Cluster for Testing.
 */
public final class CreateDatabase {

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
      createDatabase(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void createDatabase(Properties properties) {
    String endpoints = properties.getProperty(HOST_PROPERTY);
    String[] endpointsArray = endpoints.split(",");
    String[] host = endpointsArray[0].split(":");
    String hostname = host[0];
    String username = properties.getProperty(USERNAME_PROPERTY);
    String password = properties.getProperty(PASSWORD_PROPERTY);

    System.err.printf("Creating database on %s as user %s", hostname, username);

    String endpoint = "/v1/bdbs";
    REST client = new REST(hostname, username, password, true, 9443);

    JsonObject parameters = getSettings();
    JsonObject result = client.jsonBody(parameters).post(endpoint).validate().json();

    String actionUid = result.get("action_uid").getAsString();
    String actionEndpoint = "/v1/actions/" + actionUid;
    if (!client.waitForJsonValue(actionEndpoint, "status", "completed", 30)) {
      throw new RuntimeException("timeout waiting for database creation to complete");
    }
  }

  private static JsonObject getSettings() {
    JsonObject settings = new JsonObject();
    JsonArray shardConfig = new JsonArray();
    JsonObject regexA = new JsonObject();
    regexA.addProperty("regex", ".*\\{(?<tag>.*)\\}.*");
    JsonObject regexB = new JsonObject();
    regexB.addProperty("regex", "(?<tag>.*)");
    shardConfig.add(regexA);
    shardConfig.add(regexB);
    settings.addProperty("aof_policy", "appendfsync-always");
    settings.addProperty("oss_cluster", true);
    settings.addProperty("oss_sharding", false);
    settings.addProperty("oss_cluster_api_preferred_ip_type", "external");
    settings.add("shard_key_regex", shardConfig);
    settings.addProperty("data_persistence", "aof");
    settings.addProperty("memory_size", 1073741824);
    settings.addProperty("name", "ycsb");
    settings.addProperty("port", 12000);
    settings.addProperty("proxy_policy", "all-nodes");
    settings.addProperty("replication", true);
    settings.addProperty("slave_ha", true);
    settings.addProperty("sharding", true);
    settings.addProperty("shards_count", 2);
    settings.addProperty("type", "redis");
    settings.addProperty("uid", 1);

    return settings;
  }

  private CreateDatabase() {
    super();
  }

}
