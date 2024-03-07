package site.ycsb;

/**
 * Return Cleanup Class.
 */
public final class TestCleanupFactory {
  private static TestCleanup singleton = null;

  public static synchronized TestCleanup newInstance(String className) {
    if (singleton == null) {
      singleton = TestCleanupFactory.initStatsClass(className);
    }
    return singleton;
  }

  public static synchronized TestCleanup getInstance() {
    return singleton;
  }

  private TestCleanupFactory() {
  }

  public static TestCleanup initStatsClass(String className) {
    ClassLoader classLoader = TestCleanupFactory.class.getClassLoader();
    TestCleanup newClass;

    try {
      Class<?> loadClass = classLoader.loadClass(className);

      newClass = (TestCleanup) loadClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    return newClass;
  }
}
