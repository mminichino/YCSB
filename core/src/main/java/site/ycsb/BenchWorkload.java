package site.ycsb;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * One experiment scenario.
 */
public abstract class BenchWorkload {
  static final Logger LOGGER =
      (Logger) LoggerFactory.getLogger("site.ycsb.BenchWorkload");
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final List<Future<Status>> tasks = new ArrayList<>();
  private ExecutorService executor = Executors.newFixedThreadPool(32);
  private boolean debug = false;
  private int threadId;

  /**
   * Initialize the scenario. Create any generators and other shared objects here.
   * Called once, in the main client thread, before any operations are started.
   */
  public void init(Properties p) throws WorkloadException {
  }

  public long prepare(SQLDB db, boolean runMode) throws WorkloadException {
    return 0;
  }

  public void initThreadPool(int count) {
    executor = Executors.newFixedThreadPool(count);
  }

  /**
   * Initialize any state for a particular client thread. Since the scenario object
   * will be shared among all threads, this is the place to create any state that is specific
   * to one thread. To be clear, this means the returned object should be created anew on each
   * call to initThread(); do not return the same object multiple times.
   * The returned object will be passed to invocations of doInsert() and doTransaction()
   * for this thread. There should be no side effects from this call; all state should be encapsulated
   * in the returned object. If you have no state to retain for this thread, return null. (But if you have
   * no state to retain for this thread, probably you don't need to override initThread().)
   * 
   * @return false if the workload knows it is done for this thread. Client will terminate the thread.
   * Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read
   * traces from a file, return true when there are more to do, false when you are done.
   */
  public boolean initThread(Properties p, int threadId, int threadCount) throws WorkloadException {
    this.threadId = threadId;
    return true;
  }

  public int getThreadId() {
    return threadId;
  }

  /**
   * Cleanup the scenario. Called once, in the main client thread, after all operations have completed.
   */
  public void cleanup() throws WorkloadException {
  }

  public abstract boolean test(BenchRun db, Object threadState);

  public abstract boolean run(BenchRun db, Object threadState);

  public void enableDebug() {
    debug = true;
  }

  /**
   * Allows scheduling a request to stop the workload.
   */
  public void requestStop() {
    stopRequested.set(true);
  }

  /**
   * Check the status of the stop request flag.
   * @return true if stop was requested, false otherwise.
   */
  public boolean workloadRunState() {
    return !stopRequested.get();
  }
}
