package site.ycsb.db.couchbase3;

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
  }

  public void createReplication() {
    createXDCRReference(target.hostname, target.username, target.password, target.external);
    createXDCRReplication(target.hostname, source.bucketName, target.bucketName);
  }

  public void removeReplication() {
    deleteXDCRReplication(target.hostname, source.bucketName, target.bucketName);
    deleteXDCRReference(target.hostname);
  }

  public void createXDCRReference(String hostname, String username, String password, Boolean external) {
    Map<String, String> parameters = new HashMap<>();

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

    try {
      RESTInterface rest = new RESTInterface(source.rallyHost, source.username, source.password,
          source.useSsl, source.adminPort);
      String endpoint = "/pools/default/remoteClusters";
      rest.postParameters(endpoint, parameters);
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
  }

  public String getXDCRReference(String hostname) {
    try {
      RESTInterface rest = new RESTInterface(source.rallyHost, source.username, source.password,
          source.useSsl, source.adminPort);
      JsonArray remotes = rest.getJSONArray("/pools/default/remoteClusters");
      for (JsonElement entry : remotes) {
        if (entry.getAsJsonObject().get("name").getAsString().equals(hostname)) {
          return entry.getAsJsonObject().get("uuid").getAsString();
        }
      }
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public void deleteXDCRReference(String hostname) {
    if (getXDCRReference(hostname) == null) {
      return;
    }

    String endpoint = "/pools/default/remoteClusters/" + hostname;

    try {
      RESTInterface rest = new RESTInterface(source.rallyHost, source.username, source.password,
          source.useSsl, source.adminPort);
      rest.deleteEndpoint(endpoint);
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
  }

  public void createXDCRReplication(String remote, String sourceBucket, String targetBucket) {
    Map<String, String> parameters = new HashMap<>();

    if (isXDCRReplicating(remote, sourceBucket, targetBucket)) {
      return;
    }

    parameters.put("replicationType", "continuous");
    parameters.put("fromBucket", sourceBucket);
    parameters.put("toCluster", remote);
    parameters.put("toBucket", targetBucket);

    try {
      RESTInterface rest = new RESTInterface(source.rallyHost, source.username, source.password,
          source.useSsl, source.adminPort);
      String endpoint = "/controller/createReplication";
      rest.postParameters(endpoint, parameters);
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteXDCRReplication(String remote, String sourceBucket, String targetBucket) {
    if (!isXDCRReplicating(remote, sourceBucket, targetBucket)) {
      return;
    }

    String uuid = getXDCRReference(remote);

    if (uuid == null) {
      return;
    }

    String endpoint = "/controller/cancelXDCR/" + uuid + "%2F" + sourceBucket + "%2F" + targetBucket;

    try {
      RESTInterface rest = new RESTInterface(source.rallyHost, source.username, source.password,
          source.useSsl, source.adminPort);
      rest.deleteEndpoint(endpoint);
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }
  }

  public Boolean isXDCRReplicating(String remote, String sourceBucket, String targetBucket) {
    String uuid = getXDCRReference(remote);

    if (uuid == null) {
      return false;
    }

    String endpoint = "/settings/replications/" + uuid + "%2F" + sourceBucket + "%2F" + targetBucket;

    try {
      RESTInterface rest = new RESTInterface(source.rallyHost, source.username, source.password,
          source.useSsl, source.adminPort);
      rest.getJSON(endpoint);
      return true;
    } catch (RESTException e) {
      if (ErrorCode.valueOf(e.getCode()) == ErrorCode.BADREQUEST) {
        return false;
      }
      throw new RuntimeException(e);
    }
  }
}
