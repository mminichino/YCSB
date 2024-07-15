package site.ycsb.tpc.tpcc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class RunDriver extends BenchWorkload {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.tpcc.RunDriver");
  public static final String QUERIES_PROPERTY = "benchmark.queryClass";
  public static final String QUERIES_PROPERTY_DEFAULT = "site.ycsb.tpc.tpcc.SQLQueries";
  public static final String QUERIES_NUM_PROPERTY = "benchmark.queryNumber";
  public static final String QUERIES_NUM_PROPERTY_DEFAULT = "0";
  public static final String QUERIES_PRINT_PROPERTY = "benchmark.queryPrint";
  public static final String QUERIES_PRINT_PROPERTY_DEFAULT = "false";
  public static final String BENCHMARK_PROPERTY = "bench.benchmark";
  public static final String BENCHMARK_PROPERTY_DEFAULT = "ch2";
  public static final String QUERY_GROUP_PROPERTY = "bench.benchQueryGroup";
  public static final String QUERY_GROUP_PROPERTY_DEFAULT = "analytics";
  private static final Object CYCLE_COORDINATOR = new Object();
  private static final AtomicLong opCounter = new AtomicLong(0);
  public static Queries queries;
  public static String queryClass;
  public static int queryNumber;
  public static boolean queryPrint;
  public static List<Integer> queryVector;
  public static boolean debug = false;

  @Override
  public void init(Properties p) throws WorkloadException {
    queryClass = p.getProperty(QUERIES_PROPERTY, QUERIES_PROPERTY_DEFAULT);
    queryNumber = Integer.parseInt(p.getProperty(QUERIES_NUM_PROPERTY, QUERIES_NUM_PROPERTY_DEFAULT));
    queryPrint = Boolean.parseBoolean(p.getProperty(QUERIES_PRINT_PROPERTY, QUERIES_PRINT_PROPERTY_DEFAULT));
    String benchmark = p.getProperty(BENCHMARK_PROPERTY, BENCHMARK_PROPERTY_DEFAULT);
    String queryGroup = p.getProperty(QUERY_GROUP_PROPERTY, QUERY_GROUP_PROPERTY_DEFAULT);

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    try {
      queries = new Queries(benchmark, queryGroup);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new WorkloadException(e);
    }

    queryVector = queries.getPermutation();
  }

  public int getNextQuery() {
    int next;
    synchronized (CYCLE_COORDINATOR) {
      long operation = opCounter.getAndIncrement();
      next = Math.toIntExact(operation % queryVector.size());
    }
    return next;
  }

  @Override
  public boolean test(BenchRun db, int threadId, Object threadState) {
    List<String> queryList = queries.getQueryList();
    try {
      for (int i = 0; i < queryList.size(); i++) {
        if (queryNumber > 0 && i != queryNumber - 1) {
          continue;
        }

        String query = queries.statement(i + 1);

        if (queryPrint) {
          System.out.printf("Query #%s:\n", i + 1);
          System.out.println(query);
          continue;
        }

        long start = System.nanoTime();
        List<ObjectNode> results = db.query(query, i + 1);
        long end = System.nanoTime();
        long elapsedTimeNano = end - start;
        double elapsedTime = (double) elapsedTimeNano / 1_000_000_000;

        if (results == null) {
          LOGGER.warn("No results found for query: {}", query);
          continue;
        }

        System.out.printf("Query %d returned %d results in %.4f seconds\n", i + 1, results.size(), elapsedTime);

        LOGGER.info("Query #{}", i + 1);
        for (ObjectNode result : results) {
          LOGGER.info("\n{}", result.toPrettyString());
        }
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return false;
    }
    return true;
  }

  @Override
  public boolean run(BenchRun db, int threadId, Object threadState) {
    int next = getNextQuery();
    int queryNum = queryVector.get(next);
    String query = queries.statement(queryNum);

    if (debug) {
      LOGGER.debug("Thread {}: Query #{}", threadId, queryNum);
    }

    try {
      List<ObjectNode> results = db.query(query, queryNum);
      if (results == null) {
        LOGGER.error("No results found for query: {}", query);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return false;
    }
  }
}
