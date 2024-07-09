package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.Record;
import site.ycsb.tpc.TPCCLoad;
import site.ycsb.tpc.Tables;

import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class TPCC extends SQLWorkload {
  public static final String TPCC_TRANSACTION_COUNT = "tpcc.transactionCount";
  public static final String TPCC_MAX_ITEMS = "tpcc.maxItems";
  public static final String TPCC_CUST_PER_DIST = "tpcc.custPerDist";
  public static final String TPCC_DIST_PER_WAREHOUSE = "tpcc.distPerWarehouse";
  public static final String TPCC_ORD_PER_DIST = "tpcc.ordPerDist";
  public static final String TPCC_MAX_NUM_ITEMS = "tpcc.maxNumItems";
  public static final String TPCC_MAX_ITEM_LEN = "tpcc.maxItemLen";

  private static int transactionCount;
  private static int maxItems;
  private static int custPerDist;
  private static int distPerWarehouse;
  private static int ordPerDist;
  private static int maxNumItems;
  private static int maxItemLen;
  private static boolean enableDebug = false;
  private static int warehouseCount = 1;

  private static boolean waitQueue = true;
  Queue<Record> queue = new LinkedList<>();

  @Override
  public void init(Properties p) throws WorkloadException {
    transactionCount = Integer.parseInt(p.getProperty("tpcc.transactionCount", "5"));
    maxItems = Integer.parseInt(p.getProperty("tpcc.maxItems", "100000"));
    custPerDist = Integer.parseInt(p.getProperty("tpcc.custPerDist", "3000"));
    distPerWarehouse = Integer.parseInt(p.getProperty("tpcc.distPerWarehouse", "10"));
    ordPerDist = Integer.parseInt(p.getProperty("tpcc.ordPerDist", "3000"));
    maxNumItems = Integer.parseInt(p.getProperty("tpcc.maxNumItems", "15"));
    maxItemLen = Integer.parseInt(p.getProperty("tpcc.maxItemLen", "24"));
    warehouseCount = Integer.parseInt(p.getProperty("tpcc.warehouseCount", "1"));
    initThreadPool(32);
  }

  @Override
  public long prepare(SQLDB db, boolean runMode) {
    db.createTable("item", Tables.itemTableC, Tables.itemKeysC);
    db.createTable("warehouse", Tables.warehouseTableC, Tables.warehouseKeysC);
    db.createTable("stock", Tables.stockTableC, Tables.stockKeysC);
    db.createTable("district", Tables.districtTableC, Tables.districtKeysC);
    db.createTable("customer", Tables.customerTableC, Tables.customerKeysC);
    db.createTable("history", Tables.historyTableC, Tables.historyKeysC);
    db.createTable("orders", Tables.orderTableC, Tables.orderKeysC);
    db.createTable("new_orders", Tables.newOrderTableC, Tables.newOrderKeysC);
    db.createTable("order_line", Tables.orderLineTableC, Tables.orderLineKeysC);

    if (runMode) {
      System.out.println("Run Mode");
      return 0;
    } else {
      TPCCLoad.TPCCLoadBuilder builder = new TPCCLoad.TPCCLoadBuilder();
      TPCCLoad tpcc = builder
          .custPerDist(custPerDist)
          .distPerWarehouse(distPerWarehouse)
          .ordPerDist(ordPerDist)
          .maxItems(maxItems)
          .enableDebug(enableDebug)
          .build();

      tpcc.loadItems(queue);
      tpcc.loadWare(queue, warehouseCount);
      tpcc.loadCust(queue, warehouseCount);
      tpcc.loadOrd(queue, warehouseCount);
      return queue.size();
    }
  }

  public void processQueue(SQLDB db) {
    while (!queue.isEmpty()) {
      Record record = queue.poll();
      taskAdd(() -> db.insert(record));
    }
  }

  @Override
  public boolean load(SQLDB db, Object threadState) {
    processQueue(db);
    taskWait();
    return false;
  }

  @Override
  public boolean run(SQLDB db, Object threadState) {
    return false;
  }
}
