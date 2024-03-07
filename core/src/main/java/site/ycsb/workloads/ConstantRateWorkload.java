package site.ycsb.workloads;
import site.ycsb.*;
import site.ycsb.generator.ExponentialGenerator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import java.util.*;

/**
 * Generate a constant rate workload.
 */

public class ConstantRateWorkload extends CoreWorkload {

  public static final String RATE_PROPERTY_DEFAULT = "10000";
  public static final String RATE_AMOUNT_PROPERTY = "transactionrate";
  private long transactionRate;
  private long transactions = 0L;
  private int retryNumber = 0;
  private double timeStamp;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final WriteLock wLock = rwLock.writeLock();

  @Override
  public void init(Properties p) throws WorkloadException {
    transactionRate = Long.parseLong(p.getProperty(RATE_AMOUNT_PROPERTY, RATE_PROPERTY_DEFAULT));
    timeStamp = System.currentTimeMillis();

    super.init(p);
  }

  private void rateWait() {
    transactions += 1;
    wLock.lock();

    long currentTime = System.currentTimeMillis();

    double deltaTime = (currentTime - timeStamp);

    long tps = (long) (transactions / deltaTime * 1000L);

    if (tps >= transactionRate && transactions !=1) {
      retryNumber += 1;
      long waitFactor = 10L;
      double factor = waitFactor * Math.pow(2, retryNumber);
      long wait = (long) factor;
      try {
        Thread.sleep(wait);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    } else {
      if (retryNumber > 0) {
        retryNumber -= 1;
      }
    }

    wLock.unlock();
  }

  @Override
  long nextKeynum() {
    rateWait();
    long keynum;
    if (keychooser instanceof ExponentialGenerator) {
      do {
        keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
      } while (keynum < 0);
    } else {
      do {
        keynum = keychooser.nextValue().intValue();
      } while (keynum > transactioninsertkeysequence.lastValue());
    }
    return keynum;
  }

}
