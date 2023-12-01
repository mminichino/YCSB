package site.ycsb.db.couchbase3;

import com.couchbase.client.java.manager.bucket.StorageBackend;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

/**
 * Couchbase Capella Utility.
 */
public class CouchbaseCapella {
  private static final String CAPELLA_API_ENDPOINT = "cloudapi.cloud.couchbase.com";
  private RESTInterface capella;
  private String project;
  private String database;
  private String apiKey;
  private String organizationId;
  private String projectId;
  private String databaseId;

  public CouchbaseCapella(String project, String database) {
    this.project = project;
    this.database = database;

    Path homePath = Paths.get(System.getProperty("user.home"));
    Path tokenFilePath = Paths.get(homePath.toString(), ".capella", "default-api-key-token.txt");

    File inputFile = new File(tokenFilePath.toString());
    Scanner input;
    try {
      input = new Scanner(inputFile);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

    while(input.hasNext()) {
      String token = input.nextLine();
      String[] items = token.split("\\s*:\\s*");
      String key = items[0];
      if (Objects.equals(key, "APIKeyToken")) {
        apiKey = items[1];
      }
    }

    capella = new RESTInterface(CAPELLA_API_ENDPOINT, apiKey, true);

    organizationId = getOrganizationId();
    projectId = getProjectId(project);
    databaseId = getDatabaseId(database);
  }

  public String getOrganizationId() {
    String endpoint = "/v4/organizations";

    List<JsonElement> result = capella.getCapella(endpoint);
    return result.get(0).getAsJsonObject().get("id").getAsString();
  }

  public String getProjectId(String project) {
    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects";

    List<JsonElement> result = capella.getCapella(endpoint);
    for (JsonElement entry : result) {
      if (Objects.equals(entry.getAsJsonObject().get("name").getAsString(), project)) {
        return entry.getAsJsonObject().get("id").getAsString();
      }
    }
    return null;
  }

  public String getDatabaseId(String database) {
    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects/" +
        projectId +
        "/clusters";

    List<JsonElement> result = capella.getCapella(endpoint);
    for (JsonElement entry : result) {
      if (Objects.equals(entry.getAsJsonObject().get("name").getAsString(), database)) {
        return entry.getAsJsonObject().get("id").getAsString();
      }
    }
    return null;
  }

  public String getBucketId(String bucket) {
    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects/" +
        projectId +
        "/clusters/" +
        databaseId +
        "/buckets";

    List<JsonElement> result = capella.getCapella(endpoint);
    for (JsonElement entry : result) {
      if (Objects.equals(entry.getAsJsonObject().get("name").getAsString(), bucket)) {
        return entry.getAsJsonObject().get("id").getAsString();
      }
    }
    return null;
  }

  public Boolean isBucket(String bucket) {
    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects/" +
        projectId +
        "/clusters/" +
        databaseId +
        "/buckets";

    List<JsonElement> result = capella.getCapella(endpoint);
    for (JsonElement entry : result) {
      if (Objects.equals(entry.getAsJsonObject().get("name").getAsString(), bucket)) {
        return true;
      }
    }
    return false;
  }

  public void createBucket(String bucket, long quota, int replicas, StorageBackend type) {
    if (isBucket(bucket)) {
      return;
    }

    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects/" +
        projectId +
        "/clusters/" +
        databaseId +
        "/buckets";

    JsonObject parameters = new JsonObject();
    parameters.addProperty("name", bucket);
    parameters.addProperty("type", "couchbase");
    parameters.addProperty("storageBackend", type == StorageBackend.COUCHSTORE ? "couchstore" : "magma");
    parameters.addProperty("memoryAllocationInMb", quota);
    parameters.addProperty("bucketConflictResolution", "seqno");
    parameters.addProperty("durabilityLevel", "none");
    parameters.addProperty("replicas", replicas);
    parameters.addProperty("flush", false);
    parameters.addProperty("timeToLiveInSeconds", 0);

    try {
      capella.postJSON(endpoint, parameters);
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
  }

  public void dropBucket(String bucket) {
    if (!isBucket(bucket)) {
      return;
    }

    String bucketId = getBucketId(bucket);

    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects/" +
        projectId +
        "/clusters/" +
        databaseId +
        "/buckets/" +
        bucketId;

    try {
      capella.deleteEndpoint(endpoint);
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
  }
}
