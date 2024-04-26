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
    Option replication = new Option("r", "replication", false, "enable HA");
    source.setRequired(true);
    options.addOption(source);
    options.addOption(replication);

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
    boolean enableHA = cmd.hasOption("replication");

    try {
      properties.load(Files.newInputStream(Paths.get(propFile)));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }

    try {
      createDatabase(properties, enableHA);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void createDatabase(Properties properties, boolean enableHA) {
    String endpoints = properties.getProperty(HOST_PROPERTY);
    String[] endpointsArray = endpoints.split(",");
    String[] host = endpointsArray[0].split(":");
    String hostname = host[0];
    String username = properties.getProperty(USERNAME_PROPERTY);
    String password = properties.getProperty(PASSWORD_PROPERTY);
    REST client = new REST(hostname, username, password, true, 9443);

    System.err.printf("Creating database on %s as user %s\n", hostname, username);

    String endpoint = "/v1/nodes";
    JsonArray node = client.get(endpoint).validate().jsonArray();
    long totalMemory = node.get(0).getAsJsonObject().get("total_memory").getAsLong();

    endpoint = "/v1/bdbs";
    JsonObject parameters = getSettings(totalMemory, enableHA);
    JsonObject result = client.jsonBody(parameters).post(endpoint).validate().json();

    String actionUid = result.get("action_uid").getAsString();
    String actionEndpoint = "/v1/actions/" + actionUid;
    if (!client.waitForJsonValue(actionEndpoint, "status", "completed", 30)) {
      throw new RuntimeException("timeout waiting for database creation to complete");
    }
  }

  private static JsonObject getSettings(long memory, boolean enableHA) {
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
    settings.addProperty("memory_size", memory);
    settings.addProperty("name", "ycsb");
    settings.addProperty("port", 12000);
    settings.addProperty("proxy_policy", "all-nodes");
    if (enableHA) {
      settings.addProperty("replication", true);
      settings.addProperty("slave_ha", true);
    } else {
      settings.addProperty("replication", false);
      settings.addProperty("slave_ha", false);
    }
    settings.addProperty("sharding", true);
    if (enableHA) {
      settings.addProperty("shards_count", 2);
    } else {
      settings.addProperty("shards_count", 4);
    }
    settings.addProperty("type", "redis");
    settings.addProperty("uid", 1);

    return settings;
  }

  private CreateDatabase() {
    super();
  }

}
