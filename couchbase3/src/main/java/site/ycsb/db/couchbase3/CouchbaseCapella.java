package site.ycsb.db.couchbase3;

import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.bucket.StorageBackend;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    while(input.hasNext())
    {
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
    List<JsonElement> result = capella.getCapella("/v4/organizations");
    return result.get(0).getAsJsonObject().get("id").getAsString();
  }

  public String getProjectId(String project) {
    List<JsonElement> result = capella.getCapella("/v4/organizations/" + organizationId + "/projects");
    for (JsonElement entry : result) {
      if (Objects.equals(entry.getAsJsonObject().get("name").getAsString(), project)) {
        return entry.getAsJsonObject().get("id").getAsString();
      }
    }
    return null;
  }

  public String getDatabaseId(String database) {
    List<JsonElement> result = capella.getCapella("/v4/organizations/"
        + organizationId + "/projects/" + projectId + "/clusters");
    for (JsonElement entry : result) {
      if (Objects.equals(entry.getAsJsonObject().get("name").getAsString(), database)) {
        return entry.getAsJsonObject().get("id").getAsString();
      }
    }
    return null;
  }

  public Boolean isBucket(String bucket) {
    List<JsonElement> result = capella.getCapella("/v4/organizations/"
        + organizationId + "/projects/" + projectId + "/clusters/" + databaseId + "/buckets");
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
    JsonObject parameters = new JsonObject();
    parameters.addProperty("name", bucket);
    parameters.addProperty("type", "couchbase");
    parameters.addProperty("storageBackend", type == StorageBackend.COUCHSTORE ? "couchstore" : "magma");
    parameters.addProperty("memoryAllocationInMb", quota);
    parameters.addProperty("bucketConflictResolution", "seqno");
    parameters.addProperty("durabilityLevel", bucket);
    parameters.addProperty("replicas", replicas);
    parameters.addProperty("flush", false);
    parameters.addProperty("timeToLiveInSeconds", 0);
  }
}
