package site.ycsb.tpc.tpcc;

import site.ycsb.Batch;
import site.ycsb.tpc.TPCCUtil;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

import java.util.*;
import java.util.stream.Stream;

public class Generate {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.tpcc.Generate");
  private final int maxItems;
  private final int custPerDist;
  private final int distPerWarehouse;
  private final int ordPerDist;
  private final int warehouseNumber;
  private final int supplierCount;
  private final int batchSize;
  private final boolean separateOrderLine;
  private final boolean enableDebug;
  private final TPCCUtil util;

  public List<Item> item = new ArrayList<>();
  public List<Warehouse> warehouse = new ArrayList<>();
  public List<Stock> stock = new ArrayList<>();
  public List<District> district = new ArrayList<>();
  public List<Customer> customer = new ArrayList<>();
  public List<History> history = new ArrayList<>();
  public List<Order> orders = new ArrayList<>();
  public List<NewOrder> newOrders = new ArrayList<>();
  public List<OrderLine> orderLine = new ArrayList<>();
  public List<Supplier> supplier = new ArrayList<>();
  public List<Nation> nation = new ArrayList<>();
  public List<Region> region = new ArrayList<>();

  public Map<Integer, Double> customerTotalMap = new HashMap<>();

  public static class GeneratorBuilder {
    private int maxItems = 100000;
    private int custPerDist = 3000;
    private int distPerWarehouse = 10;
    private int ordPerDist = 3000;
    private int warehouseNumber = 1;
    private int supplierCount = 10000;
    private int batchSize = 1000;
    private boolean separateOrderLine = false;
    private boolean enableDebug = false;

    public GeneratorBuilder maxItems(int value) {
      this.maxItems = value;
      return this;
    }

    public GeneratorBuilder custPerDist(int value) {
      this.custPerDist = value;
      return this;
    }

    public GeneratorBuilder distPerWarehouse(int value) {
      this.distPerWarehouse = value;
      return this;
    }

    public GeneratorBuilder ordPerDist(int value) {
      this.ordPerDist = value;
      return this;
    }

    public GeneratorBuilder warehouseNumber(int value) {
      this.warehouseNumber = value;
      return this;
    }

    public GeneratorBuilder supplierCount(int value) {
      this.supplierCount = value;
      return this;
    }

    public GeneratorBuilder batchSize(int value) {
      this.batchSize = value;
      return this;
    }

    public GeneratorBuilder separateOrderLine(boolean value) {
      this.separateOrderLine = value;
      return this;
    }

    public GeneratorBuilder enableDebug(boolean value) {
      this.enableDebug = value;
      return this;
    }

    public Generate build() {
      return new Generate(this);
    }
  }

  private Generate(GeneratorBuilder builder) {
    this.maxItems = builder.maxItems;
    this.custPerDist = builder.custPerDist;
    this.distPerWarehouse = builder.distPerWarehouse;
    this.ordPerDist = builder.ordPerDist;
    this.warehouseNumber = builder.warehouseNumber;
    this.supplierCount = builder.supplierCount;
    this.batchSize = builder.batchSize;
    this.separateOrderLine = builder.separateOrderLine;
    this.enableDebug = builder.enableDebug;
    this.util = new TPCCUtil(custPerDist, ordPerDist);
  }

  public void createSchema() {
    if (warehouseNumber == 1) {
      generateItems();
      generateSuppliers();
      generateNation();
      generateRegion();
    }

    generateWarehouse(warehouseNumber);
    generateDistrict(warehouseNumber);
  }

  public Stream<List<Item>> itemData() {
    return Batch.split(item, batchSize);
  }

  public Stream<List<Warehouse>> warehouseData() {
    return Batch.split(warehouse, batchSize);
  }

  public Stream<List<Stock>> stockData() {
    return Batch.split(stock, batchSize);
  }

  public Stream<List<District>> districtData() {
    return Batch.split(district, batchSize);
  }

  public Stream<List<Customer>> customerData() {
    return Batch.split(customer, batchSize);
  }

  public Stream<List<History>> historyData() {
    return Batch.split(history, batchSize);
  }

  public Stream<List<Order>> orderData() {
    return Batch.split(orders, batchSize);
  }

  public Stream<List<NewOrder>> newOrderData() {
    return Batch.split(newOrders, batchSize);
  }

  public Stream<List<OrderLine>> orderLineData() {
    return Batch.split(orderLine, batchSize);
  }

  public Stream<List<Supplier>> supplierData() {
    return Batch.split(supplier, batchSize);
  }

  public Stream<List<Nation>> nationData() {
    return Batch.split(nation, batchSize);
  }

  public Stream<List<Region>> regionData() {
    return Batch.split(region, batchSize);
  }

  public void generateItems() {
    int[] orig = new int[maxItems + 1];
    int pos;
    int i;

    LOGGER.debug("begin item table data generation");

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }
    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = util.randomNumber(0, maxItems);
      } while (orig[pos] != 0);
      orig[pos] = 1;
    }

    for (int i_id = 1; i_id <= maxItems; i_id++) {
      item.add(new Item(i_id, util, orig));
    }

    LOGGER.debug("item table data generation complete");
  }

  public void generateWarehouse(int warehouseNum) {
    LOGGER.debug("begin warehouse {} data generation", warehouseNum);

    warehouse.add(new Warehouse(warehouseNum, util));
    generateStock(warehouseNum);
    generateDistrict(warehouseNum);

    LOGGER.debug("warehouse {} table data generation complete", warehouseNum);
  }

  public void generateStock(int warehouseNum) {
    int[] orig = new int[maxItems + 1];
    int pos;
    int i;

    LOGGER.debug("begin stock data generation for warehouse {}", warehouseNum);

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }

    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = util.randomNumber(0, maxItems);
      } while (orig[pos] != 0);
      orig[pos] = 1;
    }

    for (int s_i_id = 1; s_i_id <= maxItems; s_i_id++) {
      stock.add(new Stock(s_i_id, warehouseNum, warehouseNumber, util, orig));
    }

    LOGGER.debug("stock table data generation complete for warehouse {}", warehouseNum);
  }

  public void generateDistrict(int warehouseNum) {
    LOGGER.debug("begin district data generation for warehouse {}", warehouseNum);

    for (int d_id = 1; d_id <= distPerWarehouse; d_id++) {
      district.add(new District(d_id, warehouseNum, util));
      generateOrders(warehouseNum, d_id);
      generateCustomers(warehouseNum, d_id);
    }

    LOGGER.debug("district table data generation complete for warehouse {}", warehouseNum);
  }

  public void generateCustomers(int warehouseNum, int districtNum) {
    LOGGER.debug("begin customer data generation for warehouse {}", warehouseNum);

    for (int c_id = 1; c_id <= custPerDist; c_id++) {
      double orderTotal = customerTotalMap.getOrDefault(c_id, 0.0);
      customer.add(new Customer(c_id, districtNum, warehouseNum, orderTotal, util));
      history.add(new History(c_id, districtNum, warehouseNum, orderTotal, util));
    }

    LOGGER.debug("customer table data generation complete for warehouse {}", warehouseNum);
  }

  public void generateOrders(int warehouseNum, int districtNum) {
    LOGGER.debug("begin order data generation for warehouse {}", warehouseNum);
    util.initPermutation();

    for (int o_id = 1; o_id <= ordPerDist; o_id++) {
      int o_ol_cnt = util.randomNumber(5, 15);
      Date orderDate = util.randomDate();
      Order order = new Order(o_id, districtNum, warehouseNum, orderDate, o_ol_cnt, maxItems, customerTotalMap, separateOrderLine, util);
      int o_c_id = order.o_c_id();
      orders.add(order);
      if (o_id > 2100) {
        newOrders.add(new NewOrder(o_id, districtNum, warehouseNum));
      }
      if (separateOrderLine) {
        for (int ol = 1; ol <= o_ol_cnt; ol++) {
          orderLine.add(new OrderLine(ol, o_id, o_c_id, districtNum, warehouseNum, orderDate, maxItems, customerTotalMap, util));
        }
      }
    }

    LOGGER.debug("order table data generation complete for warehouse {}", warehouseNum);
  }

  public void generateSuppliers() {
    LOGGER.debug("begin supplier data generation");
    int[] nationVector = new int[util.numNations()];
    Arrays.fill(nationVector, 0);
    List<List<Integer>> subsets = util.getRandomSets(supplierCount, 5);
    List<Integer> comments = subsets.get(0);
    List<Integer> complaints = subsets.get(1);

    for (int su_suppkey = 1; su_suppkey <= supplierCount; su_suppkey++) {
      supplier.add(new Supplier(su_suppkey, comments, complaints, nationVector, util));
    }
  }

  public void generateNation() {
    LOGGER.debug("begin nation data generation");

    for (int nationkey = 1; nationkey <= util.numNations(); nationkey++) {
      nation.add(new Nation(nationkey, util));
    }
  }

  public void generateRegion() {
    LOGGER.debug("begin region data generation");

    for (int r_regionkey = 1; r_regionkey <= util.numRegions(); r_regionkey++) {
      region.add(new Region(r_regionkey, util));
    }
  }
}
