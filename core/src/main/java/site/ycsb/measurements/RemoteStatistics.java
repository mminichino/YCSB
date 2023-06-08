package site.ycsb.measurements;

/**
 * Collects database statistics by API, and reports them when requested.
 */
public abstract class RemoteStatistics {

  public void init() {
  }

  public abstract void startCollectionThread(String hostName, String userName,
                                             String password, String instance, Boolean useTls);

  public abstract void stopCollectionThread();

  public abstract void getResults();

}
