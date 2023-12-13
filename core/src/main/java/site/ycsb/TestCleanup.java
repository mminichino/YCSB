package site.ycsb;

import java.util.Properties;

/**
 * Cleanup after test run.
 */
public abstract class TestCleanup {
  public abstract void testClean(Properties properties);
}
