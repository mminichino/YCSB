package site.ycsb.db.couchbase3;

import com.couchbase.client.java.http.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Prepare Cluster for Eventing.
 */
public final class CouchbaseEventing {
  private final CouchbaseConnect db;
  private final CouchbaseHttpClient client;

  /**
   * Class Builder.
   */
  public static class EventingBuilder {
    private CouchbaseConnect targetDb;

    public EventingBuilder database(final CouchbaseConnect db) {
      this.targetDb = db;
      return this;
    }

    public CouchbaseEventing build() {
      return new CouchbaseEventing(this);
    }
  }

  private CouchbaseEventing(CouchbaseEventing.EventingBuilder builder) {
    this.db = builder.targetDb;
    this.client = this.db.getHttpClient();
  }

  public Boolean isEventingFunction(String name) {
    String endpoint = "/api/v1/functions/" + name;
    HttpResponse response = client.get(
            HttpTarget.eventing(),
            HttpPath.of(endpoint));

    return response.success();
  }

  public Boolean isEventingFunctionDeployed(String name) {
    String endpoint = "/api/v1/functions/" + name;
    HttpResponse response = client.get(
            HttpTarget.eventing(),
            HttpPath.of(endpoint));

    Gson gson = new Gson();
    JsonObject result = gson.fromJson(response.contentAsString(), JsonObject.class);

    if (!response.success()) {
      throw new RuntimeException("Can not get eventing function status: "
              + response.statusCode() + ": " + response.contentAsString());
    } else {
      return result.get("settings").getAsJsonObject().get("deployment_status").getAsBoolean();
    }
  }

  public void deployEventingFunction(String scriptFile, String metaBucket) {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL inputFile = classloader.getResource(scriptFile);

    String fileName = inputFile != null ? inputFile.getFile() : null;

    if (fileName == null) {
      throw new RuntimeException("Can not find script file");
    }

    File funcFile = new File(fileName);
    String[] fileParts = funcFile.getName().split("\\.");
    String name = fileParts[0];

    byte[] encoded;
    try {
      encoded = Files.readAllBytes(Paths.get(funcFile.getAbsolutePath()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    JsonObject parameters = generateParameters(metaBucket, encoded, name);

    if (!isEventingFunction(name)) {
      String endpoint = "/api/v1/functions/" + name;
      HttpResponse response = client.post(
              HttpTarget.eventing(),
              HttpPath.of(endpoint),
              HttpPostOptions.httpPostOptions()
                      .body(HttpBody.json(parameters.toString())));

      if (!response.success()) {
        throw new RuntimeException("Can not create eventing function: "
                + response.statusCode() + ": " + response.contentAsString());
      }
    }

    if (!isEventingFunctionDeployed(name)) {
      String endpoint = "/api/v1/functions/" + name + "/deploy";
      HttpResponse response = client.post(
              HttpTarget.eventing(),
              HttpPath.of(endpoint));

      if (!response.success()) {
        throw new RuntimeException("Can not deploy eventing function: "
                + response.statusCode() + ": " + response.contentAsString());
      }
    }
  }

  public void undeployEventingFunction(String scriptFile) {
    String[] fileParts = scriptFile.split("\\.");
    String name = fileParts[0];

    if (isEventingFunctionDeployed(name)) {
      String endpoint = "/api/v1/functions/" + name + "/undeploy";
      HttpResponse response = client.post(
              HttpTarget.eventing(),
              HttpPath.of(endpoint));

      if (!response.success()) {
        throw new RuntimeException("Can not undeploy eventing function: "
                + response.statusCode() + ": " + response.contentAsString());
      }
    }

    if (isEventingFunction(name)) {
      String endpoint = "/api/v1/functions/" + name;
      HttpResponse response = client.delete(
              HttpTarget.eventing(),
              HttpPath.of(endpoint));

      if (!response.success()) {
        throw new RuntimeException("Can not delete eventing function: "
                + response.statusCode() + ": " + response.contentAsString());
      }
    }
  }

  @NotNull
  private JsonObject generateParameters(String metaBucket, byte[] encoded, String name) {
    String fileContents = new String(encoded, StandardCharsets.UTF_8);

    JsonObject parameters = new JsonObject();
    parameters.addProperty("appcode", fileContents);
    JsonObject depConfig = getDepConfig(metaBucket);
    parameters.add("depcfg", depConfig);
    parameters.addProperty("enforce_schema", false);
    parameters.addProperty("appname", name);
    JsonObject settingsConfig = getSettingsConfig();
    parameters.add("settings", settingsConfig);
    JsonObject functionConfig = new JsonObject();
    functionConfig.addProperty("bucket", "*");
    functionConfig.addProperty("scope", "*");
    parameters.add("function_scope", functionConfig);
    return parameters;
  }

  @NotNull
  private static JsonObject getSettingsConfig() {
    JsonObject settingsConfig = new JsonObject();
    settingsConfig.addProperty("dcp_stream_boundary", "everything");
    settingsConfig.addProperty("description", "Auto Added Function");
    settingsConfig.addProperty("execution_timeout", 60);
    settingsConfig.addProperty("language_compatibility", "6.6.2");
    settingsConfig.addProperty("log_level", "INFO");
    settingsConfig.addProperty("n1ql_consistency", "none");
    settingsConfig.addProperty("processing_status", false);
    settingsConfig.addProperty("timer_context_size", 1024);
    settingsConfig.addProperty("worker_count", 16);
    return settingsConfig;
  }

  @NotNull
  private JsonObject getDepConfig(String metaBucket) {
    JsonObject depConfig = new JsonObject();
    JsonObject bucketConfig = new JsonObject();
    bucketConfig.addProperty("alias", "collection");
    bucketConfig.addProperty("bucket_name", db.getBucketName());
    bucketConfig.addProperty("scope_name", db.getScopeName());
    bucketConfig.addProperty("collection_name", db.getCollectionName());
    bucketConfig.addProperty("access", "rw");
    JsonArray buckets = new JsonArray();
    buckets.add(bucketConfig);
    depConfig.add("buckets", buckets);
    depConfig.addProperty("source_bucket", db.getBucketName());
    depConfig.addProperty("source_scope", db.getScopeName());
    depConfig.addProperty("source_collection", db.getCollectionName());
    depConfig.addProperty("metadata_bucket", metaBucket);
    depConfig.addProperty("metadata_scope", "_default");
    depConfig.addProperty("metadata_collection", "_default");
    return depConfig;
  }
}
