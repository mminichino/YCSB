package site.ycsb.db.couchbase3;

import com.couchbase.client.java.http.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.util.HashMap;
import java.util.Map;

/**
 * Prepare Cluster for XDCR.
 */
public final class CouchbaseXDCR {
  private final CouchbaseConnect source;
  private final CouchbaseConnect target;
  private final CouchbaseHttpClient client;

  /**
   * Class Builder.
   */
  public static class XDCRBuilder {
    private CouchbaseConnect sourceDb;
    private CouchbaseConnect targetDb;

    public XDCRBuilder source(final CouchbaseConnect db) {
      this.sourceDb = db;
      return this;
    }

    public XDCRBuilder target(final CouchbaseConnect db) {
      this.targetDb = db;
      return this;
    }

    public CouchbaseXDCR build() {
      return new CouchbaseXDCR(this);
    }
  }

  private CouchbaseXDCR(CouchbaseXDCR.XDCRBuilder builder) {
    this.source = builder.sourceDb;
    this.target = builder.targetDb;
    this.client = this.source.getHttpClient();
  }

  public void createReplication() {
    createXDCRReference();
    createXDCRReplication();
  }

  public void removeReplication() {
    deleteXDCRReplication();
    deleteXDCRReference();
  }

  public void createXDCRReference() {
    Map<String, String> parameters = new HashMap<>();
    String hostname = target.hostValue();
    String username = target.userValue();
    String password = target.passwordValue();
    boolean external = target.externalValue();

    if (getXDCRReference(hostname) != null) {
      return;
    }

    parameters.put("name", hostname);
    parameters.put("hostname", hostname);
    parameters.put("username", username);
    parameters.put("password", password);
    if (external) {
      parameters.put("network_type", "external");
    }

    HttpResponse response = client.post(
            HttpTarget.manager(),
            HttpPath.of("/pools/default/remoteClusters"),
            HttpPostOptions.httpPostOptions()
                    .body(HttpBody.form(parameters)));

    if (!response.success()) {
      throw new RuntimeException("Can not create XDCR reference: "
              + response.statusCode() + ": " + response.contentAsString());
    }
  }

  public String getXDCRReference(String hostname) {
    HttpResponse response = client.get(
            HttpTarget.manager(),
            HttpPath.of("/pools/default/remoteClusters"));

    Gson gson = new Gson();
    JsonArray remotes = gson.fromJson(response.contentAsString(), JsonArray.class);

    for (JsonElement entry : remotes) {
      if (entry.getAsJsonObject().get("name").getAsString().equals(hostname)) {
        return entry.getAsJsonObject().get("uuid").getAsString();
      }
    }

    if (!response.success()) {
      throw new RuntimeException("Can not get XDCR references: "
              + response.statusCode() + ": " + response.contentAsString());
    }

    return null;
  }

  public void deleteXDCRReference() {
    String hostname = target.hostValue();

    if (getXDCRReference(hostname) == null) {
      return;
    }

    String endpoint = "/pools/default/remoteClusters/" + hostname;

    HttpResponse response = client.delete(
            HttpTarget.manager(),
            HttpPath.of(endpoint));

    if (!response.success()) {
      throw new RuntimeException("Can not delete XDCR references: "
              + response.statusCode() + ": " + response.contentAsString());
    }
  }

  public void createXDCRReplication() {
    Map<String, String> parameters = new HashMap<>();
    String remote = target.hostValue();
    String sourceBucket = source.getBucketName();
    String targetBucket = target.getBucketName();

    if (isXDCRReplicating(remote, sourceBucket, targetBucket)) {
      return;
    }

    parameters.put("replicationType", "continuous");
    parameters.put("fromBucket", sourceBucket);
    parameters.put("toCluster", remote);
    parameters.put("toBucket", targetBucket);

    HttpResponse response = client.post(
            HttpTarget.manager(),
            HttpPath.of("/controller/createReplication"),
            HttpPostOptions.httpPostOptions()
                    .body(HttpBody.form(parameters)));

    if (!response.success()) {
      throw new RuntimeException("Can not create XDCR replication: "
              + response.statusCode() + ": " + response.contentAsString());
    }
  }

  public void deleteXDCRReplication() {
    String remote = target.hostValue();
    String sourceBucket = source.getBucketName();
    String targetBucket = target.getBucketName();

    if (!isXDCRReplicating(remote, sourceBucket, targetBucket)) {
      return;
    }

    String uuid = getXDCRReference(remote);

    if (uuid == null) {
      return;
    }

    String endpoint = "/controller/cancelXDCR/" + uuid + "%2F" + sourceBucket + "%2F" + targetBucket;

    HttpResponse response = client.delete(
            HttpTarget.manager(),
            HttpPath.of(endpoint));

    if (!response.success()) {
      throw new RuntimeException("Can not delete XDCR replication: "
              + response.statusCode() + ": " + response.contentAsString());
    }
  }

  public Boolean isXDCRReplicating(String remote, String sourceBucket, String targetBucket) {
    String uuid = getXDCRReference(remote);

    if (uuid == null) {
      return false;
    }

    String endpoint = "/settings/replications/" + uuid + "%2F" + sourceBucket + "%2F" + targetBucket;

    HttpResponse response = client.get(
            HttpTarget.manager(),
            HttpPath.of(endpoint));

    if (response.statusCode() == 400) {
      return false;
    } else if (!response.success()) {
      throw new RuntimeException("Can not check XDCR replication: "
              + response.statusCode() + ": " + response.contentAsString());
    } else {
      return true;
    }
  }
}
