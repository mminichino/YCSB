package site.ycsb.db.couchbase3;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.RemoteStatistics;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private ScheduledFuture<?> apiHandle = null;

  @Override
  public void init() {
    super.init();
  }

  @Override
  public void startCollectionThread(String hostName, String userName, String password) {
    String url = String.format("http://%s:8091/pools/default", hostName);
    Runnable callApi = new Runnable() {
      public void run() {
        OkHttpClient client = new OkHttpClient();
        String credential = Credentials.basic(userName, password);
        Request request = new Request.Builder()
            .url(url)
            .header("Authorization", credential)
            .build();

        try {
          ResponseBody response = client.newCall(request).execute().body();
          if (response != null) {
            HashMap data = new ObjectMapper().readValue(response.string(), HashMap.class);
            List nodeList = (List) data.get("nodes");
            for (Object element : nodeList) {
              HashMap entry = (HashMap) element;
              HashMap systemStats = (HashMap) data.get("systemStats");
              System.err.println(systemStats.get("cpu_utilization_rate"));
            }
          }
        } catch (IOException e) {
          System.err.println(e.getMessage());
          LOGGER.error(e.getMessage());
        }
      }
    };
    System.err.println("Starting remote statistics thread...");
    apiHandle = scheduler.scheduleWithFixedDelay(callApi, 0, 30, SECONDS);
  }

  @Override
  public void stopCollectionThread() {
    apiHandle.cancel(true);
  }

  @Override
  public void getResults() {

  }
}
