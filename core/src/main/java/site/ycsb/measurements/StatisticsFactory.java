package site.ycsb.measurements;

/**
 * Return Statistics Class.
 */
public final class StatisticsFactory {
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
