/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import site.ycsb.measurements.Measurements;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A thread for executing transactions or data inserts to the database.
 */
public class BenchClientThread implements Runnable {
  // Counts down each of the clients completing.
  private final CountDownLatch completeLatch;

  private static boolean spinSleep;
  private final BenchRun db;
  private final boolean testMode;
  private final BenchWorkload workload;
  private final int opcount;
  private double targetOpsPerMs;

  private int opsDone;
  private int threadID;
  private int threadcount;
  private final Properties props;
  private long targetOpsTickNs;
  private final Measurements measurements;
  private final AtomicInteger opsCounter;

  /**
   * Constructor.
   *
   * @param db                   the DB implementation to use
   * @param testMode       true to do transactions, false to insert data
   * @param workload             the workload to use
   * @param props                the properties defining the experiment
   * @param opcount              the number of operations (transactions or inserts) to do
   * @param targetPerThreadPerMS target number of operations per thread per ms
   * @param completeLatch        The latch tracking the completion of all clients.
   */
  public BenchClientThread(BenchRun db, boolean testMode, BenchWorkload workload, Properties props, int opcount,
                           double targetPerThreadPerMS, CountDownLatch completeLatch, AtomicInteger opsCounter) {
    this.db = db;
    this.testMode = testMode;
    this.workload = workload;
    this.opcount = opcount;
    this.opsCounter = opsCounter;
    opsDone = 0;
    if (targetPerThreadPerMS > 0) {
      targetOpsPerMs = targetPerThreadPerMS;
      targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
    }
    this.props = props;
    measurements = Measurements.getMeasurements();
    spinSleep = Boolean.parseBoolean(this.props.getProperty("spin.sleep", "false"));
    this.completeLatch = completeLatch;
  }

  public void setThreadId(final int threadId) {
    threadID = threadId;
  }

  public void setThreadCount(final int threadCount) {
    threadcount = threadCount;
  }

  public int getOpsDone() {
    return opsCounter.get();
  }

  @Override
  public void run() {
    try {
      db.init();
    } catch (DBException e) {
      System.err.println(Arrays.toString(e.getStackTrace()));
      return;
    }

    Object workloadState;
    try {
      workloadState = workload.initThread(props, threadID, threadcount);
    } catch (WorkloadException e) {
      System.err.println(Arrays.toString(e.getStackTrace()));
      return;
    }

    if ((targetOpsPerMs > 0) && (targetOpsPerMs <= 1.0)) {
      long randomMinorDelay = ThreadLocalRandom.current().nextInt((int) targetOpsTickNs);
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }
    try {
      long startTimeNanos = System.nanoTime();

      while (((opcount == 0) || (opsDone < opcount)) && workload.workloadRunState()) {

        if (!testMode) {
          if (!workload.run(db, workloadState)) {
            break;
          }
        } else {
          if (!workload.test(db, workloadState)) {
            break;
          }
        }

        opsDone++;

        throttleNanos(startTimeNanos);
      }

    } catch (Exception e) {
      System.err.println(Arrays.toString(e.getStackTrace()));
      System.exit(0);
    }

    try {
      measurements.setIntendedStartTimeNs(0);
      db.cleanup();
    } catch (DBException e) {
      System.err.println(Arrays.toString(e.getStackTrace()));
    } finally {
      completeLatch.countDown();
    }
  }

  private static void sleepUntil(long deadline) {
    while (System.nanoTime() < deadline) {
      if (!spinSleep) {
        LockSupport.parkNanos(deadline - System.nanoTime());
      }
    }
  }

  private void throttleNanos(long startTimeNanos) {
    //throttle the operations
    if (targetOpsPerMs > 0) {
      // delay until next tick
      long deadline = startTimeNanos + opsDone * targetOpsTickNs;
      sleepUntil(deadline);
      measurements.setIntendedStartTimeNs(deadline);
    }
  }

  /**
   * The total amount of work this thread is still expected to do.
   */
  int getOpsTodo() {
    int todo = opcount - opsDone;
    return Math.max(todo, 0);
  }
}
