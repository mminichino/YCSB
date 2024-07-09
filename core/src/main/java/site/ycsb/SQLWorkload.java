/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * One experiment scenario. One object of this type will
 * be instantiated and shared among all client threads. This class
 * should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 */
public abstract class SQLWorkload {
  static final Logger LOGGER =
      (Logger) LoggerFactory.getLogger("site.ycsb.SQLWorkload");
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final List<Future<Status>> tasks = new ArrayList<>();
  private ExecutorService executor = Executors.newFixedThreadPool(32);
  private boolean debug = false;

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
    return true;
  }

  /**
   * Cleanup the scenario. Called once, in the main client thread, after all operations have completed.
   */
  public void cleanup() throws WorkloadException {
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads, this
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations and mutations on threadState. Mutations to threadState do not need to be
   * synchronized, since each thread has its own threadState instance.
   */
  public abstract boolean load(SQLDB db, Object threadState);

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations and mutations on threadState. Mutations to threadState do not need to be
   * synchronized, since each thread has its own threadState instance.
   * 
   * @return false if the workload knows it is done for this thread. Client will terminate the thread. 
   * Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read
   * traces from a file, return true when there are more to do, false when you are done.
   */
  public abstract boolean run(SQLDB db, Object threadState);

  public void enableDebug() {
    debug = true;
  }

  public void taskAdd(Callable<Status> task) {
    tasks.add(executor.submit(task));
  }

  public boolean taskWait() {
    boolean status = true;
    for (Future<Status> future : tasks) {
      try {
        Status result = future.get();
        if (debug) {
          LOGGER.debug("Task status: {}", result);
        }
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error(e.getMessage(), e);
        status = false;
      }
    }
    tasks.clear();
    return status;
  }

  public void stopThreadPool() {
    executor.shutdown();
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
