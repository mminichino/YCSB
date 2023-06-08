package site.ycsb.db.couchbase3;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.RemoteStatistics;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
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
  private final String dateFormat = "yyyy-MM-dd HH:mm:ss";
  private final SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
  private final TrustManager[] trustAllCerts = new TrustManager[]{
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
        }

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return new java.security.cert.X509Certificate[]{};
        }
      }
  };

  @Override
  public void init() {
    super.init();
  }

  @Override
  public void startCollectionThread(String hostName, String userName, String password, String bucket, Boolean tls) {
    String prefix;
    String port;
    if (tls) {
      prefix = "https";
      port = "18091";
    } else {
      prefix = "http";
      port = "8091";
    }
    String urlSystem = String.format("%s://%s:%s/pools/default", prefix, hostName, port);
    String urlBucket = String.format("%s://%s:%s/pools/default/buckets/%s/stats", prefix, hostName, port, bucket);
    SSLContext sslContext;
    try {
      sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      LOGGER.error(e.getMessage(), e);
      return;
    }
    final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

    STATISTICS.info(String.format("==== Begin Cluster Collection %s ====\n", timeStampFormat.format(new Date())));

    Runnable callApi = new Runnable() {
      public void run() {
        OkHttpClient client = new OkHttpClient.Builder()
            .sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0])
            .build();
        String credential = Credentials.basic(userName, password);
        StringBuilder output = new StringBuilder();

        Request sysRequest = new Request.Builder()
            .url(urlSystem)
            .header("Authorization", credential)
            .build();

        try {
          ResponseBody response = client.newCall(sysRequest).execute().body();
          if (response != null) {
            double cpu = 0;
            double mem = 0;
            double free = 0;
            int count = 0;
            HashMap<?, ?> data = new ObjectMapper().readValue(response.string(), HashMap.class);
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
          }
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }

        Request bucketRequest = new Request.Builder()
            .url(urlBucket)
            .header("Authorization", credential)
            .build();

        try {
          ResponseBody response = client.newCall(bucketRequest).execute().body();
          if (response != null) {
            HashMap<?, ?> data = new ObjectMapper().readValue(response.string(), HashMap.class);
            LinkedHashMap<?, ?> op = (LinkedHashMap<?, ?>) data.get("op");
            LinkedHashMap<?, ?> samples = (LinkedHashMap<?, ?>) op.get("samples");

            Map<String, String> itemsToGet = createBucketStatMap();
            for (Map.Entry<String, String> dataPoint : itemsToGet.entrySet()) {
              ArrayList<?> dpList = (ArrayList<?>) samples.get(dataPoint.getValue());
              double average = averageArray(dpList);
              output.append(String.format("%s: %.1f ", dataPoint.getKey(), average));
            }
          }
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
        STATISTICS.info(String.format("%s\n", output));
        output.delete(0, output.length());
      }
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
    newMap.put("DiskCommit", "avg_disk_commit_time");
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
