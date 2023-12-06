package site.ycsb.db.couchbase3;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.RemoteStatistics;

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
  /**
   * Metric Type.
   */
  public enum MetricMode {
    SYSTEM, BUCKET, XDCR, DISK
  }

  @Override
  public void init() {
    super.init();
  }

  @Override
  public void startCollectionThread(String hostName, String userName, String password, String bucket, Boolean tls) {
    int port;
    DecimalFormat formatter = new DecimalFormat("#,###");

    if (tls) {
      port = 18091;
    } else {
      port = 8091;
    }

    RESTInterface rest = new RESTInterface(hostName, userName, password, tls, port);

    STATISTICS.info(String.format("==== Begin Cluster Collection %s ====\n", timeStampFormat.format(new Date())));

    Runnable callApi = () -> {
      StringBuilder output = new StringBuilder();
      String timeStamp = timeStampFormat.format(new Date());
      output.append(String.format("%s ", timeStamp));

      JsonArray metrics = new JsonArray();
      addMetric("sys_cpu_host_utilization_rate", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucket);
      addMetric("sys_mem_total", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucket);
      addMetric("sys_mem_free", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucket);
      addMetric("n1ql_active_requests", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucket);
      addMetric("n1ql_load", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucket);
      addMetric("n1ql_request_time", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucket);
      addMetric("n1ql_service_time", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucket);

      addMetric("kv_ep_num_non_resident", MetricFunction.MAX, metrics, MetricMode.BUCKET, bucket);
      addMetric("kv_ep_queue_size", MetricFunction.MAX, metrics, MetricMode.BUCKET, bucket);
      addMetric("kv_ep_flusher_todo", MetricFunction.MAX, metrics, MetricMode.BUCKET, bucket);

      addMetric("kv_ep_io_total_read_bytes_bytes", MetricFunction.MAX, metrics, MetricMode.DISK, bucket);
      addMetric("kv_ep_io_total_write_bytes_bytes", MetricFunction.MAX, metrics, MetricMode.DISK, bucket);
      addMetric("kv_curr_items", MetricFunction.MAX, metrics, MetricMode.DISK, bucket);

      addMetric("xdcr_resp_wait_time_seconds", MetricFunction.MAX, metrics, MetricMode.XDCR, bucket);
      addMetric("xdcr_resp_wait_time_seconds", MetricFunction.AVG, metrics, MetricMode.XDCR, bucket);
      addMetric("xdcr_wtavg_docs_latency_seconds", MetricFunction.MAX, metrics, MetricMode.XDCR, bucket);
      addMetric("xdcr_data_replicated_bytes", MetricFunction.MAX, metrics, MetricMode.XDCR, bucket);

      try {
        String endpoint = "/pools/default/stats/range";
        JsonArray data = rest.postJSONArray(endpoint, metrics);

        double cpuUtil = getMetricAvgDouble(data.getAsJsonArray().get(0).getAsJsonObject());
        long memTotal = getMetricMaxLong(data.getAsJsonArray().get(1).getAsJsonObject());
        long memFree = getMetricMaxLong(data.getAsJsonArray().get(2).getAsJsonObject());
        long sqlReq  = getMetricMaxLong(data.getAsJsonArray().get(3).getAsJsonObject());
        long sqlLoad  = getMetricMaxLong(data.getAsJsonArray().get(4).getAsJsonObject());
        long sqlReqTime  = getMetricMaxLong(data.getAsJsonArray().get(5).getAsJsonObject());
        long sqlSvcTime  = getMetricMaxLong(data.getAsJsonArray().get(6).getAsJsonObject());

        double nonResident = getMetricAvgDouble(data.getAsJsonArray().get(7).getAsJsonObject());
        double queueSize = getMetricAvgDouble(data.getAsJsonArray().get(8).getAsJsonObject());
        double flushTodo = getMetricAvgDouble(data.getAsJsonArray().get(9).getAsJsonObject());

        double bytesRead = getMetricDiffDouble(data.getAsJsonArray().get(10).getAsJsonObject());
        double bytesWrite = getMetricDiffDouble(data.getAsJsonArray().get(11).getAsJsonObject());
        long curItems = getMetricMaxLong(data.getAsJsonArray().get(12).getAsJsonObject());

        double xdcrMax = getMetricAvgDouble(data.getAsJsonArray().get(13).getAsJsonObject());
        double xdcrAvg = getMetricAvgDouble(data.getAsJsonArray().get(14).getAsJsonObject());
        double wtavgMax = getMetricAvgDouble(data.getAsJsonArray().get(15).getAsJsonObject());
        double xdcrBytes = getMetricDiffDouble(data.getAsJsonArray().get(16).getAsJsonObject());

        long queueTotal = (long) queueSize + (long) flushTodo;

        output.append(String.format("CPU: %.1f ", cpuUtil));
        output.append(String.format("Mem: %s ", formatDataSize(memTotal)));
        output.append(String.format("Free: %s ", formatDataSize(memFree)));
        output.append(String.format("sqlReq: %d ", sqlReq));
        output.append(String.format("sqlLoad: %d ", sqlLoad));
        output.append(String.format("sqlReqTime: %d ", sqlReqTime));
        output.append(String.format("sqlSvcTime: %d ", sqlSvcTime));

        output.append(String.format("Resident: %d %% ", 100L - (long) nonResident));

        output.append(String.format("Queue: %s ", formatter.format(queueTotal)));
        output.append(String.format("Read: %s ", formatDataSize(bytesRead)));
        output.append(String.format("Write: %s ", formatDataSize(bytesWrite)));
        output.append(String.format("Items: %s ", formatter.format(curItems)));

        output.append(String.format("XDCRMax: %.1f ", xdcrMax * 1000));
        output.append(String.format("XDCRAvg: %.1f ", xdcrAvg * 1000));
        output.append(String.format("wtavgMax: %.1f ", wtavgMax * 1000));
        output.append(String.format("Bandwidth: %s ", formatDataSize(xdcrBytes)));

      } catch (RESTException e) {
        if (ErrorCode.valueOf(e.getCode()) != ErrorCode.FORBIDDEN) {
          throw new RuntimeException(e);
        }
      }

      STATISTICS.info(String.format("%s\n", output));
      output.delete(0, output.length());
    };
    System.err.println("Starting remote statistics thread...");
    apiHandle = scheduler.scheduleWithFixedDelay(callApi, 0, 30, SECONDS);
  }

  private static void addMetric(String name, MetricFunction func, JsonArray metrics, MetricMode mode, String bucket) {
    JsonObject block = new JsonObject();
    JsonArray metric = new JsonArray();
    JsonObject definition = new JsonObject();
    definition.addProperty("label", "name");
    definition.addProperty("value", name);
    metric.add(definition);
    if (mode == MetricMode.BUCKET || mode == MetricMode.DISK) {
      JsonObject bucketDefinition = new JsonObject();
      bucketDefinition.addProperty("label", "bucket");
      bucketDefinition.addProperty("value", bucket);
      metric.add(bucketDefinition);
    } else if (mode == MetricMode.XDCR) {
      JsonObject source = new JsonObject();
      source.addProperty("label", "sourceBucketName");
      source.addProperty("value", bucket);
      metric.add(source);
      JsonObject pipeLine = new JsonObject();
      pipeLine.addProperty("label", "pipelineType");
      pipeLine.addProperty("value", "Main");
      metric.add(pipeLine);
    }
    JsonArray applyFunctions = new JsonArray();
    applyFunctions.add(func.getValue());
    block.add("metric", metric);
    block.add("applyFunctions", applyFunctions);
    if (mode == MetricMode.DISK) {
      block.addProperty("nodesAggregation", "sum");
    } else {
      block.addProperty("nodesAggregation", "avg");
    }
    block.addProperty("alignTimestamps", true);
    block.addProperty("step", 15);
    block.addProperty("start", -60);
    metrics.add(block);
  }

  private double getMetricAvgDouble(JsonObject block) {
    double metric = 0.0;
    if (!block.get("data").getAsJsonArray().asList().isEmpty()) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      for (JsonElement entry : values) {
        metric += Double.parseDouble(entry.getAsJsonArray().get(1).getAsString());
      }
      return metric / values.size();
    }
    return 0.0;
  }

  private long getMetricAvgLong(JsonObject block) {
    long metric = 0;
    if (!block.get("data").getAsJsonArray().asList().isEmpty()) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      for (JsonElement entry : values) {
        metric += Double.parseDouble(entry.getAsJsonArray().get(1).getAsString());
      }
      return metric / values.size();
    }
    return 0;
  }

  private long getMetricMaxLong(JsonObject block) {
    long metric = 0;
    if (!block.get("data").getAsJsonArray().asList().isEmpty()) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      for (JsonElement entry : values) {
        long thisMetric = (long) Double.parseDouble(entry.getAsJsonArray().get(1).getAsString());
        if (thisMetric > metric) {
          metric = thisMetric;
        }
      }
    }
    return metric;
  }

  private long getMetricSumLong(JsonObject block) {
    long metric = 0;
    if (!block.get("data").getAsJsonArray().asList().isEmpty()) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      for (JsonElement entry : values) {
        metric += (long) Double.parseDouble(entry.getAsJsonArray().get(1).getAsString());
      }
    }
    return metric;
  }

  private double getMetricDiffDouble(JsonObject block) {
    double metric = 0.0;
    if (!block.get("data").getAsJsonArray().asList().isEmpty()) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      for (int i = 0; i < values.size() - 1; i++) {
        double a = Double.parseDouble(values.getAsJsonArray().get(i).getAsJsonArray().get(1).getAsString());
        double b = Double.parseDouble(values.getAsJsonArray().get(i+1).getAsJsonArray().get(1).getAsString());
        metric += ((b - a) / 15);
      }
      return metric / (values.size() - 1);
    }
    return metric;
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
