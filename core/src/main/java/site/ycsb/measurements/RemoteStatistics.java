package site.ycsb.measurements;

import java.util.Properties;

/**
 * Collects database statistics by API, and reports them when requested.
 */
public abstract class RemoteStatistics {

  public void init(Properties properties) {
  }

  public abstract void startCollectionThread();

  public abstract void stopCollectionThread();

  public abstract void getResults();

}
