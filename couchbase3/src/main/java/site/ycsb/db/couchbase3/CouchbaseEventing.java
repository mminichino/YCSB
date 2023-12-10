package site.ycsb.db.couchbase3;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

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
  }

  public Boolean isEventingFunction(String name) throws CouchbaseConnectException {
    RESTInterface eventing = new RESTInterface(db.hostValue(), db.userValue(), db.passwordValue(),
        db.sslValue(), db.getEventingPort());
    String endpoint = "/api/v1/functions/" + name;

    try {
      eventing.getJSON(endpoint);
      return true;
    } catch (RESTException e) {
      return false;
    }
  }

  public Boolean isEventingFunctionDeployed(String name) throws CouchbaseConnectException {
    RESTInterface eventing = new RESTInterface(db.hostValue(), db.userValue(), db.passwordValue(),
        db.sslValue(), db.getEventingPort());
    String endpoint = "/api/v1/functions/" + name;

    try {
      JsonObject result = eventing.getJSON(endpoint);
      return result.get("settings").getAsJsonObject().get("deployment_status").getAsBoolean();
    } catch (RESTException e) {
      return false;
    }
  }

  public void deployEventingFunction(String scriptFile, String metaBucket)
      throws CouchbaseConnectException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL inputFile = classloader.getResource(scriptFile);

    String fileName = inputFile != null ? inputFile.getFile() : null;

    if (fileName == null) {
      throw new CouchbaseConnectException("Can not find script file");
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
    String fileContents = new String(encoded, StandardCharsets.UTF_8);

    JsonObject parameters = new JsonObject();
    parameters.addProperty("appcode", fileContents);
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
    parameters.add("depcfg", depConfig);
    parameters.addProperty("enforce_schema", false);
    parameters.addProperty("appname", name);
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
    parameters.add("settings", settingsConfig);
    JsonObject functionConfig = new JsonObject();
    functionConfig.addProperty("bucket", "*");
    functionConfig.addProperty("scope", "*");
    parameters.add("function_scope", functionConfig);

    RESTInterface eventing = new RESTInterface(db.hostValue(), db.userValue(), db.passwordValue(),
        db.sslValue(), db.getEventingPort());

    if (!isEventingFunction(name)) {
      try {
        String endpoint = "/api/v1/functions/" + name;
        eventing.postJSON(endpoint, parameters);
      } catch (RESTException e) {
        throw new RuntimeException(e);
      }
    }

    if (!isEventingFunctionDeployed(name)) {
      try {
        String endpoint = "/api/v1/functions/" + name + "/deploy";
        eventing.postEndpoint(endpoint);
      } catch (RESTException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
