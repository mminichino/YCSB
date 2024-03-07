package site.ycsb.db.couchbase3;

import com.couchbase.client.java.manager.bucket.StorageBackend;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Stream;
import java.util.Properties;

/**
 * Couchbase Capella Utility.
 */
public class CouchbaseCapella {
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  private static final String API_ENDPOINT_PROPERTY = "capella.api.endpoint";
  private static final String AUTH_FILE_PROPERTY = "capella.api.authfile";
  private static final String AUTH_FILE_NAME = "default-api-key-token.txt";
  private static final String CAPELLA_API_ENDPOINT = "cloudapi.cloud.couchbase.com";
  private final RESTInterface capella;
  private String apiKey;
  private final String organizationId;
  private final String projectId;
  private final String databaseId;

  public CouchbaseCapella(String project, String database) {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Path homePath = Paths.get(System.getProperty("user.home"));
    Properties properties = new Properties();

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        properties.load(propFile.openStream());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    String authFileName = properties.getProperty(AUTH_FILE_PROPERTY, AUTH_FILE_NAME);
    String apiEndpoint = properties.getProperty(API_ENDPOINT_PROPERTY, CAPELLA_API_ENDPOINT);

    Path tokenFilePath = Paths.get(homePath.toString(), ".capella", authFileName);

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

    capella = new RESTInterface(apiEndpoint, apiKey, true);

    organizationId = getOrganizationId();
    if (organizationId == null) throw new RuntimeException("Can not get Organization ID");
    projectId = getProjectId(project);
    if (projectId == null) throw new RuntimeException("Can not get Project ID for project " + project);
    databaseId = getDatabaseId(database);
    if (databaseId == null) throw new RuntimeException("Can not get Database ID for cluster " + database);
  }

  public String getOrganizationId() {
    String endpoint = "/v4/organizations";

    List<JsonElement> result = capella.getCapella(endpoint);
    return result.get(0).getAsJsonObject().get("id").getAsString();
  }

  public String extractId(String name, List<JsonElement> list) {
    Stream<JsonElement> stream = list.parallelStream();
    return stream.filter(e -> e.getAsJsonObject().get("name").getAsString().equals(name))
            .findFirst()
            .map(e -> e.getAsJsonObject().get("id").getAsString())
            .orElse(null);
  }

  public String getProjectId(String project) {
    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects";

    return extractId(project, capella.getCapellaList(endpoint));
  }

  public String getDatabaseId(String database) {
    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects/" +
        projectId +
        "/clusters";

    return extractId(database, capella.getCapellaList(endpoint));
  }

  public String getBucketId(String bucket) {
    String endpoint = "/v4/organizations/" +
        organizationId +
        "/projects/" +
        projectId +
        "/clusters/" +
        databaseId +
        "/buckets";

    return extractId(bucket, capella.getCapella(endpoint));
  }

  public Boolean isBucket(String bucket) {
    return getBucketId(bucket) != null;
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

    int result = capella.jsonBody(parameters).post(endpoint).code();

    if (result != 201) {
      throw new RuntimeException("Bucket create failed with code " + result);
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

    int result = capella.delete(endpoint).code();

    if (result != 204) {
      throw new RuntimeException("Bucket drop failed with code " + result);
    }
  }
}
