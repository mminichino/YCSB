package site.ycsb;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BenchLoad {
  static final Logger LOGGER =
      (Logger) LoggerFactory.getLogger("site.ycsb.BenchLoad");
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);

  public abstract void setProperties(Properties p);

  public abstract void init();

  public abstract void cleanup();

  public abstract void load();

  public void requestStop() {
    stopRequested.set(true);
  }

  public boolean workloadRunState() {
    return !stopRequested.get();
  }
}
