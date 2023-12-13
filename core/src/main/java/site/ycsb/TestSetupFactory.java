package site.ycsb;

/**
 * Return Setup Class.
 */
public final class TestSetupFactory {
  private static TestSetup singleton = null;

  public static synchronized TestSetup newInstance(String className) {
    if (singleton == null) {
      singleton = TestSetupFactory.initStatsClass(className);
    }
    return singleton;
  }

  public static synchronized TestSetup getInstance() {
    return singleton;
  }

  private TestSetupFactory() {
  }

  public static TestSetup initStatsClass(String className) {
    ClassLoader classLoader = TestSetupFactory.class.getClassLoader();
    TestSetup newClass;

    try {
      Class<?> loadClass = classLoader.loadClass(className);

      newClass = (TestSetup) loadClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    return newClass;
  }
}
