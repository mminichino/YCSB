package site.ycsb.measurements;

/**
 * Return Statistics Class.
 */
public final class StatisticsFactory {

  private static RemoteStatistics singleton = null;

  public static synchronized RemoteStatistics newInstance(String className) {
    if (singleton == null) {
      singleton = StatisticsFactory.initStatsClass(className);
    }
    return singleton;
  }

  public static synchronized RemoteStatistics getInstance() {
    return singleton;
  }

  private StatisticsFactory() {
  }

  public static RemoteStatistics initStatsClass(String className) {
    ClassLoader classLoader = StatisticsFactory.class.getClassLoader();
    RemoteStatistics newClass;

    try {
      Class<?> loadClass = classLoader.loadClass(className);

      newClass = (RemoteStatistics) loadClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    return newClass;
  }
}
