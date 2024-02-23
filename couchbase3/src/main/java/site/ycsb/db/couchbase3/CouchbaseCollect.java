package site.ycsb.db.couchbase3;

import com.couchbase.client.java.http.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.micrometer.core.instrument.util.IOUtils;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.RemoteStatistics;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Collects database statistics by API, and reports them when requested.
 */
public class CouchbaseCollect extends RemoteStatistics {

  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseCollect");
  protected static final ch.qos.logback.classic.Logger STATISTICS =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.statistics");
  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";
  private static final String STATISTICS_CONFIG_FILE = "statistics.json";
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
  private static JsonArray metricList;
  private static final DecimalFormat metricFormat = new DecimalFormat("0.##");
  private static final Map<String, MetricValue> resultMatrix = new LinkedHashMap<>();

  /**
   * Metric Mode.
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
        properties.load(propFile.openStream());
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

    URL configFile = classloader.getResource(STATISTICS_CONFIG_FILE);
    try {
      if (configFile != null) {
        String configJson = IOUtils.toString(configFile.openStream(), StandardCharsets.UTF_8);
        Gson gson = new Gson();
        metricList = gson.fromJson(configJson, JsonArray.class);
        for (JsonElement metric : metricList) {
          String name = metric.getAsJsonObject().get("metric").getAsString();
          boolean rate = metric.getAsJsonObject().get("rate").getAsBoolean();
          MetricType type = MetricType.valueOf(metric.getAsJsonObject().get("type").getAsString());
          resultMatrix.put(name, new MetricValue(type, rate));
        }
      } else {
        throw new RuntimeException("Can not access statistics config file");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void startCollectionThread() {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseHttpClient client = dbBuilder.connect(hostname, username, password).ssl(sslMode).build().getHttpClient();
    JsonArray metricSet = new JsonArray();

    for (JsonElement metric : metricList) {
      String name = metric.getAsJsonObject().get("metric").getAsString();
      MetricMode mode = MetricMode.valueOf(metric.getAsJsonObject().get("mode").getAsString());
      MetricType type = MetricType.valueOf(metric.getAsJsonObject().get("type").getAsString());
      metricSet.add(addMetric(name, mode, type));
    }

    String metricRequestBody = metricSet.toString();

    STATISTICS.info(String.format("==== Begin Cluster Collection %s ====\n", timeStampFormat.format(new Date())));

    Runnable callApi = () -> {
      StringBuilder line = new StringBuilder();
      IntStream metricStream = IntStream.range(0, metricList.size());
      String timeStamp = timeStampFormat.format(new Date());
      line.append(String.format("%s ", timeStamp));

      try {
        String endpoint = "/pools/default/stats/range";
        HttpResponse response = client.post(
                HttpTarget.manager(),
                HttpPath.of(endpoint),
                HttpPostOptions.httpPostOptions()
                        .body(HttpBody.json(metricRequestBody)));
        Gson gson = new Gson();
        JsonArray data = gson.fromJson(response.contentAsString(), JsonArray.class);

        metricStream.parallel().forEach(i -> {
          JsonElement metric = metricList.get(i);
          JsonObject result = data.getAsJsonArray().get(i).getAsJsonObject();
          String name = metric.getAsJsonObject().get("metric").getAsString();
          resultMatrix.get(name).setValue(result);
        });

        for (int i = 0; i < metricList.size(); i++) {
          JsonElement metric = metricList.get(i);
          String name = metric.getAsJsonObject().get("metric").getAsString();
          String divisor = metric.getAsJsonObject().get("divisor").isJsonNull() ? null
              : metric.getAsJsonObject().get("divisor").getAsString();
          String transform = metric.getAsJsonObject().get("transform").getAsString();
          String label = metric.getAsJsonObject().get("label").getAsString();
          line.append(generate(name, divisor, transform, label));
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        line.append(String.format("Error: %s", e.getMessage()));
      }

      STATISTICS.info(String.format("%s\n", line));
      line.delete(0, line.length());
    };

    System.err.println("Starting remote statistics thread...");
    apiHandle = scheduler.scheduleWithFixedDelay(callApi, 1, 10, SECONDS);
  }

  private static JsonObject addMetric(String name, MetricMode mode, MetricType type) {
    JsonObject block = new JsonObject();
    JsonArray metric = getMetricStruct(name, bucketName, mode);

    JsonArray applyFunctions = new JsonArray();
    if (type == MetricType.COUNTER) {
      applyFunctions.add("max");
    } else {
      applyFunctions.add("avg");
    }

    block.add("metric", metric);
    block.add("applyFunctions", applyFunctions);

    if (type == MetricType.COUNTER || type == MetricType.TOTAL) {
      block.addProperty("nodesAggregation", "sum");
    } else {
      block.addProperty("nodesAggregation", "avg");
    }

    block.addProperty("step", 1);
    block.addProperty("start", -1);

    return block;
  }

  private static JsonArray getMetricStruct(String name, String bucketName, MetricMode mode) {
    JsonArray metric = new JsonArray();

    JsonObject definition = new JsonObject();
    definition.addProperty("label", "name");
    definition.addProperty("value", name);
    metric.add(definition);

    if (mode == MetricMode.BUCKET) {
      JsonObject bucketDefinition = new JsonObject();
      bucketDefinition.addProperty("label", "bucket");
      bucketDefinition.addProperty("value", bucketName);
      metric.add(bucketDefinition);
    } else if (mode == MetricMode.XDCR) {
      JsonObject source = new JsonObject();
      source.addProperty("label", "sourceBucketName");
      source.addProperty("value", bucketName);
      metric.add(source);
      JsonObject pipeLine = new JsonObject();
      pipeLine.addProperty("label", "pipelineType");
      pipeLine.addProperty("value", "Main");
      metric.add(pipeLine);
    }
    return metric;
  }

  public String generate(String metric, String divisor, String transform, String label) {
    double finalValue = resultMatrix.get(metric).getValue();
    String valueString;

    if (divisor != null) {
      if (resultMatrix.containsKey(divisor)) {
        double value = resultMatrix.get(divisor).getValue();
        finalValue = value > 0 ? finalValue / value : finalValue;
      } else {
        LOGGER.warn("Divisor metric " + divisor + " not found");
      }
    }

    switch (transform) {
      case "to_gib":
        valueString = formatDataSize(finalValue);
        break;
      case "from_ns":
        valueString = fromNanoSeconds(finalValue);
        break;
      case "inverse":
        valueString = percentInv(finalValue);
        break;
      case "comma_delim":
        valueString = commaDelimit(finalValue);
        break;
      case "to_ms":
        valueString = toMilliSeconds(finalValue);
        break;
      case "percent":
        valueString = percentage(finalValue);
        break;
      default:
        valueString = defaultFormat(finalValue);
    }

    return label + ": " + valueString + " ";
  }

  private String defaultFormat(double value) {
    return metricFormat.format(value);
  }

  private String toMilliSeconds(double seconds) {
    double value = seconds * 1000;
    return metricFormat.format(value) + " ms";
  }

  private String commaDelimit(double value) {
    DecimalFormat formatter = new DecimalFormat("#,###.##");
    return formatter.format(value);
  }

  private String percentInv(double percentage) {
    double value = 100 - percentage;
    return metricFormat.format(value) + " %";
  }

  private String percentage(double percentage) {
    return metricFormat.format(percentage) + " %";
  }

  private String fromNanoSeconds(double nano) {
    double value = nano / 1000000;
    return metricFormat.format(value) + " ms";
  }

  private String formatDataSize(double bytes) {
    String output;

    double k = bytes / 1024.0;
    double m = ((bytes / 1024.0) / 1024.0);
    double g = (((bytes / 1024.0) / 1024.0) / 1024.0);
    double t = ((((bytes / 1024.0) / 1024.0) / 1024.0) / 1024.0);

    if (t > 1) {
      output = Math.round(t) + " TB";
    } else if (g > 1) {
      output = Math.round(g) + " GB";
    } else if (m > 1) {
      output = Math.round(m) + " MB";
    } else if (k > 1) {
      output = Math.round(k) + " KB";
    } else {
      output = Math.round(bytes) + " B";
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
