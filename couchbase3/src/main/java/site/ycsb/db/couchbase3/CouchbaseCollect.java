package site.ycsb.db.couchbase3;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.RemoteStatistics;

import java.lang.reflect.Type;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Collects database statistics by API, and reports them when requested.
 */
public class CouchbaseCollect extends RemoteStatistics {

  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.couchbase.CouchbaseClient");
  protected static final ch.qos.logback.classic.Logger STATISTICS =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("com.couchbase.statistics");
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private ScheduledFuture<?> apiHandle = null;
  private final String dateFormat = "yy-MM-dd'T'HH:mm:ss";
  private final SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);

  @Override
  public void init() {
    super.init();
  }

  @Override
  public void startCollectionThread(String hostName, String userName, String password, String bucket, Boolean tls) {
    int port;
    String remoteUUID = null;

    if (tls) {
      port = 18091;
    } else {
      port = 8091;
    }

    RESTInterface rest = new RESTInterface(hostName, userName, password, tls, port);

    try {
      JsonArray remotes = rest.getJSONArray("/pools/default/remoteClusters");
      if (!remotes.isEmpty()) {
        remoteUUID = remotes.get(0).getAsJsonObject().get("uuid").getAsString();
      }
    } catch (RESTException e) {
      throw new RuntimeException(e);
    }

    String finalRemoteUUID = remoteUUID;

    STATISTICS.info(String.format("==== Begin Cluster Collection %s ====\n", timeStampFormat.format(new Date())));

    Runnable callApi = () -> {
      StringBuilder output = new StringBuilder();
      String timeStamp = timeStampFormat.format(new Date());
      output.append(String.format("%s ", timeStamp));

      try {
        double cpu = 0;
        double mem = 0;
        double free = 0;
        int count = 0;
        HashMap<?, ?> data = rest.getMap("/pools/default");
        ArrayList<?> nodeList = (ArrayList<?>) data.get("nodes");
        for (Object element : nodeList) {
          LinkedHashMap<?, ?> sysEntry = (LinkedHashMap<?, ?>) element;
          LinkedHashMap<?, ?> systemStats = (LinkedHashMap<?, ?>) sysEntry.get("systemStats");
          cpu += wildCardToDouble(systemStats.get("cpu_utilization_rate"));
          mem += wildCardToDouble(systemStats.get("mem_total"));
          free += wildCardToDouble(systemStats.get("mem_free"));
          count++;
        }
        output.append(String.format("CPU: %.1f Mem: %s Free: %s ",
            cpu / count, formatDataSize(mem), formatDataSize(free)));
      } catch (RESTException i) {
        output.append(String.format(" ** Error: %s", i.getMessage()));
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }

      try {
        HashMap<?, ?> data = rest.getMap("/pools/default/buckets/" + bucket + "/stats");
        LinkedHashMap<?, ?> op = (LinkedHashMap<?, ?>) data.get("op");
        LinkedHashMap<?, ?> samples = (LinkedHashMap<?, ?>) op.get("samples");

        Map<String, String> itemsToGet = createBucketStatMap();
        for (Map.Entry<String, String> dataPoint : itemsToGet.entrySet()) {
          ArrayList<?> dpList = (ArrayList<?>) samples.get(dataPoint.getValue());
          double average = averageArray(dpList);
          output.append(String.format("%s: %.1f ", dataPoint.getKey(), average));
        }
      } catch (RESTException i) {
        output.append(String.format(" ** Error: %s", i.getMessage()));
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }

      if (finalRemoteUUID != null) {
        try {
          String endpoint = "/pools/default/buckets/ycsb/stats/replications%2F" +
              finalRemoteUUID +
              "%2F" +
              bucket +
              "%2F" +
              bucket +
              "%2Fwtavg_docs_latency";
          JsonObject data = rest.getJSON(endpoint);
          ArrayList<Long> values = new ArrayList<>();
          Gson gson = new Gson();
          if (data.has("nodeStats")) {
            for (Map.Entry<String, JsonElement> entry : data.get("nodeStats").getAsJsonObject().entrySet()) {
              Type listType = new TypeToken<ArrayList<Long>>() {}.getType();
              ArrayList<Long> nodeList = gson.fromJson(entry.getValue(), listType);
              values.addAll(nodeList);
            }
            double average = averageArray(values);
            output.append(String.format("XDCRLag: %.1f ", average));
          }
        } catch (RESTException e) {
          throw new RuntimeException(e);
        }
      }

      STATISTICS.info(String.format("%s\n", output));
      output.delete(0, output.length());
    };
    System.err.println("Starting remote statistics thread...");
    apiHandle = scheduler.scheduleWithFixedDelay(callApi, 0, 30, SECONDS);
  }

  private static String formatDataSize(double bytes) {
    long size = Double.valueOf(bytes).longValue();
    String output;

    double k = size/1024.0;
    double m = ((size/1024.0)/1024.0);
    double g = (((size/1024.0)/1024.0)/1024.0);
    double t = ((((size/1024.0)/1024.0)/1024.0)/1024.0);

    DecimalFormat dec = new DecimalFormat("0.00");

    if (t>1) {
      output = dec.format(t).concat(" TB");
    } else if (g>1) {
      output = dec.format(g).concat(" GB");
    } else if (m>1) {
      output = dec.format(m).concat(" MB");
    } else if (k>1) {
      output = dec.format(k).concat(" KB");
    } else {
      output = dec.format((double) size).concat(" B");
    }

    return output;
  }

  private double wildCardToDouble(Object value) {
    if (value instanceof Integer) {
      return Double.parseDouble(value.toString());
    } else if (value instanceof Double) {
      return (double) value;
    } else if (value instanceof Long) {
      return ((Long) value).doubleValue();
    } else if (value instanceof Float) {
      return (double) value;
    } else {
      return 0.0D;
    }
  }

  private double averageArray(ArrayList<?> list) {
    double total = 0;
    int count = 0;
    for (Object value : list) {
      total += wildCardToDouble(value);
      count++;
    }
    return total / count;
  }

  private static Map<String, String> createBucketStatMap() {
    Map<String, String> newMap = new HashMap<>();
    newMap.put("HitRatio", "hit_ratio");
    newMap.put("CacheMiss", "ep_cache_miss_rate");
    newMap.put("ResidentItems", "ep_resident_items_rate");
    newMap.put("QueueAge", "vb_avg_total_queue_age");
    newMap.put("CommitTime", "avg_disk_commit_time");
    newMap.put("DiskCommit", "disk_commit_total");
    newMap.put("DiskUpdate", "disk_update_total");
    newMap.put("DiskQueue", "ep_diskqueue_items");
    newMap.put("WaitTime", "avg_bg_wait_time");
    newMap.put("BytesRead", "bytes_read");
    newMap.put("BytesWriten", "bytes_written");
    newMap.put("BadCAS", "cas_badval");
    newMap.put("Get", "cmd_get");
    newMap.put("Lookup", "cmd_lookup");
    newMap.put("Set", "cmd_set");
    newMap.put("Con", "curr_connections");
    newMap.put("Items", "curr_items_tot");
    newMap.put("WriteQueue", "disk_write_queue");
    newMap.put("DCPReplica", "ep_dcp_replica_total_bytes");
    newMap.put("DCPOther", "ep_dcp_other_total_bytes");
    return newMap;
  }

  @Override
  public void stopCollectionThread() {
    System.err.println("Stopping remote statistics thread...");
    STATISTICS.info(String.format("==== End Cluster Collection %s ====\n", timeStampFormat.format(new Date())));
    apiHandle.cancel(true);
  }

  @Override
  public void getResults() {

  }
}
