package site.ycsb.db.couchbase3;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.RemoteStatistics;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
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
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.couchbase.CouchbaseCollect");
  protected static final ch.qos.logback.classic.Logger STATISTICS =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("com.couchbase.statistics");
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  public static final String COUCHBASE_HOST = "couchbase.hostname";
  public static final String COUCHBASE_USER = "couchbase.username";
  public static final String COUCHBASE_PASSWORD = "couchbase.password";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SSL_MODE = "couchbase.sslMode";
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private ScheduledFuture<?> apiHandle = null;
  private final String dateFormat = "yy-MM-dd'T'HH:mm:ss";
  private final SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
  private static String hostname;
  private static String username;
  private static String password;
  private static String bucketName;
  private static boolean sslMode;

  /**
   * Metric Type.
   */
  public enum MetricMode {
    SYSTEM, BUCKET, XDCR, DISK
  }

  @Override
  public void init(Properties props) {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;
    Properties properties = new Properties();

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        properties.load(Files.newInputStream(Paths.get(propFile.getFile())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    properties.putAll(props);

    hostname = properties.getProperty(COUCHBASE_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    username = properties.getProperty(COUCHBASE_USER, CouchbaseConnect.DEFAULT_USER);
    password = properties.getProperty(COUCHBASE_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    bucketName = properties.getProperty(COUCHBASE_BUCKET, "ycsb");
    sslMode = properties.getProperty(COUCHBASE_SSL_MODE, "false").equals("true");
  }

  @Override
  public void startCollectionThread() {
    int port;
    DecimalFormat formatter = new DecimalFormat("#,###");

    if (sslMode) {
      port = 18091;
    } else {
      port = 8091;
    }

    RESTInterface rest = new RESTInterface(hostname, username, password, sslMode, port);

    STATISTICS.info(String.format("==== Begin Cluster Collection %s ====\n", timeStampFormat.format(new Date())));

    Runnable callApi = () -> {
      StringBuilder output = new StringBuilder();
      String timeStamp = timeStampFormat.format(new Date());
      output.append(String.format("%s ", timeStamp));

      JsonArray metrics = new JsonArray();
      addMetric("sys_cpu_host_utilization_rate", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);
      addMetric("sys_mem_total", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);
      addMetric("sys_mem_free", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);
      addMetric("n1ql_requests", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);
      addMetric("n1ql_active_requests", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);
      addMetric("n1ql_load", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);
      addMetric("n1ql_request_time", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);
      addMetric("n1ql_service_time", MetricFunction.MAX, metrics, MetricMode.SYSTEM, bucketName);

      addMetric("kv_ep_num_non_resident", MetricFunction.MAX, metrics, MetricMode.BUCKET, bucketName);
      addMetric("kv_ep_queue_size", MetricFunction.MAX, metrics, MetricMode.BUCKET, bucketName);
      addMetric("kv_ep_flusher_todo", MetricFunction.MAX, metrics, MetricMode.BUCKET, bucketName);

      addMetric("kv_ep_io_total_read_bytes_bytes", MetricFunction.MAX, metrics, MetricMode.DISK, bucketName);
      addMetric("kv_ep_io_total_write_bytes_bytes", MetricFunction.MAX, metrics, MetricMode.DISK, bucketName);
      addMetric("kv_curr_items", MetricFunction.MAX, metrics, MetricMode.DISK, bucketName);

      addMetric("xdcr_resp_wait_time_seconds", MetricFunction.MAX, metrics, MetricMode.XDCR, bucketName);
      addMetric("xdcr_resp_wait_time_seconds", MetricFunction.AVG, metrics, MetricMode.XDCR, bucketName);
      addMetric("xdcr_wtavg_docs_latency_seconds", MetricFunction.MAX, metrics, MetricMode.XDCR, bucketName);
      addMetric("xdcr_data_replicated_bytes", MetricFunction.MAX, metrics, MetricMode.XDCR, bucketName);

      try {
        String endpoint = "/pools/default/stats/range";
        JsonArray data = rest.postJSONArray(endpoint, metrics);

        double cpuUtil = getMetricAvgDouble(data.getAsJsonArray().get(0).getAsJsonObject());
        long memTotal = getMetricMaxLong(data.getAsJsonArray().get(1).getAsJsonObject());
        long memFree = getMetricMaxLong(data.getAsJsonArray().get(2).getAsJsonObject());
        long sqlReq = getMetricDiffSum(data.getAsJsonArray().get(3).getAsJsonObject());
        long sqlActiveReq  = getMetricMaxLong(data.getAsJsonArray().get(4).getAsJsonObject());
        long sqlLoad  = getMetricMaxLong(data.getAsJsonArray().get(5).getAsJsonObject());
        long sqlReqTime  = getMetricDiffSum(data.getAsJsonArray().get(6).getAsJsonObject());
        long sqlSvcTime  = getMetricDiffSum(data.getAsJsonArray().get(7).getAsJsonObject());

        double nonResident = getMetricAvgDouble(data.getAsJsonArray().get(8).getAsJsonObject());
        double queueSize = getMetricAvgDouble(data.getAsJsonArray().get(9).getAsJsonObject());
        double flushTodo = getMetricAvgDouble(data.getAsJsonArray().get(10).getAsJsonObject());

        double bytesRead = getMetricDiffDouble(data.getAsJsonArray().get(11).getAsJsonObject());
        double bytesWrite = getMetricDiffDouble(data.getAsJsonArray().get(12).getAsJsonObject());
        long curItems = getMetricMaxLong(data.getAsJsonArray().get(13).getAsJsonObject());

        double xdcrMax = getMetricAvgDouble(data.getAsJsonArray().get(14).getAsJsonObject());
        double xdcrAvg = getMetricAvgDouble(data.getAsJsonArray().get(15).getAsJsonObject());
        double wtavgMax = getMetricAvgDouble(data.getAsJsonArray().get(16).getAsJsonObject());
        double xdcrBytes = getMetricDiffDouble(data.getAsJsonArray().get(17).getAsJsonObject());

        long queueTotal = (long) queueSize + (long) flushTodo;
        double sqlReqAvgTime = ((double) sqlReqTime / 1000000) / sqlReq;
        double sqlSvcAvgTime = ((double) sqlSvcTime / 1000000) / sqlReq;

        output.append(String.format("CPU: %.1f ", cpuUtil));
        output.append(String.format("Mem: %s ", formatDataSize(memTotal)));
        output.append(String.format("Free: %s ", formatDataSize(memFree)));
        output.append(String.format("sqlReq: %d ", sqlReq));
        output.append(String.format("sqlActReq: %d ", sqlActiveReq));
        output.append(String.format("sqlLoad: %d ", sqlLoad));
        output.append(String.format("sqlReqTime: %.2f ", Double.isNaN(sqlReqAvgTime) ? 0.0 : sqlReqAvgTime));
        output.append(String.format("sqlSvcTime: %.2f ", Double.isNaN(sqlSvcAvgTime) ? 0.0 : sqlSvcAvgTime));

        output.append(String.format("Resident: %d %% ", 100L - (long) nonResident));

        output.append(String.format("Queue: %s ", formatter.format(queueTotal)));
        output.append(String.format("Read: %s ", formatDataSize(bytesRead)));
        output.append(String.format("Write: %s ", formatDataSize(bytesWrite)));
        output.append(String.format("Items: %s ", formatter.format(curItems)));

        output.append(String.format("XDCRMax: %.1f ", xdcrMax * 1000));
        output.append(String.format("XDCRAvg: %.1f ", xdcrAvg * 1000));
        output.append(String.format("wtavgMax: %.1f ", wtavgMax * 1000));
        output.append(String.format("Bandwidth: %s ", formatDataSize(xdcrBytes)));

      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        output.append(String.format("Error: %s", e.getMessage()));
      }

      STATISTICS.info(String.format("%s\n", output));
      output.delete(0, output.length());
    };
    System.err.println("Starting remote statistics thread...");
    apiHandle = scheduler.scheduleWithFixedDelay(callApi, 10, 10, SECONDS);
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
    int counter = 0;
    if (!block.get("data").getAsJsonArray().asList().isEmpty()) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      for (int i = 0; i < values.size() - 1; i++) {
        double a = Double.parseDouble(values.getAsJsonArray().get(i).getAsJsonArray().get(1).getAsString());
        double b = Double.parseDouble(values.getAsJsonArray().get(i+1).getAsJsonArray().get(1).getAsString());
        if (b > a) {
          metric += ((b - a) / 15);
          counter += 1;
        }
      }
      return metric / counter;
    }
    return metric;
  }

  private long getMetricDiffSum(JsonObject block) {
    double metric = 0.0;
    if (!block.get("data").getAsJsonArray().asList().isEmpty()) {
      JsonArray values = block.get("data").getAsJsonArray().get(0).getAsJsonObject().get("values").getAsJsonArray();
      for (int i = 0; i < values.size() - 1; i++) {
        double a = Double.parseDouble(values.getAsJsonArray().get(i).getAsJsonArray().get(1).getAsString());
        double b = Double.parseDouble(values.getAsJsonArray().get(i+1).getAsJsonArray().get(1).getAsString());
        if (b > a) {
          metric += (b - a);
        }
      }
      return Double.isNaN(metric) ? 0 : Math.round(metric);
    }
    return 0;
  }

  private static String formatDataSize(double bytes) {
    long size = Double.isNaN(bytes) ? 0 : Double.isInfinite(bytes) ? Long.MAX_VALUE : Math.round(bytes);
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
