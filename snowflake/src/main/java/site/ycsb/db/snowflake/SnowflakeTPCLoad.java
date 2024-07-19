package site.ycsb.db.snowflake;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;
import site.ycsb.*;
import site.ycsb.db.snowflake.flavors.DBFlavor;

import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import site.ycsb.tpc.tpcc.*;

public class SnowflakeTPCLoad extends LoadDriver {

  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.snowflake.SnowflakeTPCLoad");

  private static final String PROPERTY_FILE = "db.properties";
  private static final String PROPERTY_TEST = "test.properties";

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The username to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  public static final String CONNECTION_WAREHOUSE = "db.warehouse";

  public static final String CONNECTION_DB = "db.db";

  public static final String CONNECTION_SCHEMA = "db.schema";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  /** SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY. */
  private boolean sqlansiScans = false;
  /** SQL Server before 2012: TOP n after the SELECT. */
  private boolean sqlserverScans = false;

  private List<Connection> conns;
  private boolean initialized = false;
  private Properties props  = new Properties();
  private int jdbcFetchSize;
  private boolean autoCommit;
  private static final String DEFAULT_PROP = "";
  private final ConcurrentMap<String, PreparedStatement> stmtMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicInteger> countMap = new ConcurrentHashMap<>();
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;
  private boolean debug = false;
  private boolean overwrite = true;

  public void dropTable(String tableName) {
    try {
      StringBuilder sql = new StringBuilder();
      Statement stmt = conns.get(0).createStatement();
      LOGGER.info("Dropping table {}", tableName);
      sql.append("DROP TABLE IF EXISTS ");
      sql.append(tableName);
      sql.append(";");
      stmt.execute(sql.toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Status createItemTable() {
    String tableName = "item";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table item (");
      sql.append(" i_id int not null,");
      sql.append(" i_im_id int,");
      sql.append(" i_name varchar(30),");
      sql.append(" i_price double,");
      sql.append(" i_data varchar(55),");
      sql.append(" PRIMARY KEY(i_id) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      LOGGER.info(t.getMessage(), t);
      return Status.ERROR;
    }
  }

  @Override
  public Status createWarehouseTable() {
    String tableName = "warehouse";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table warehouse (");
      sql.append(" w_id int not null,");
      sql.append(" w_name varchar(11),");
      sql.append(" w_street_1 varchar(21),");
      sql.append(" w_street_2 varchar(21),");
      sql.append(" w_city varchar(21),");
      sql.append(" w_state varchar(3),");
      sql.append(" w_zip varchar(10),");
      sql.append(" w_tax double,");
      sql.append(" w_ytd double,");
      sql.append(" primary key (w_id) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createStockTable() {
    String tableName = "stock";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table stock (");
      sql.append(" s_i_id int not null,");
      sql.append(" s_w_id int not null,");
      sql.append(" s_quantity int,");
      sql.append(" s_dist_01 varchar(24),");
      sql.append(" s_dist_02 varchar(24),");
      sql.append(" s_dist_03 varchar(24),");
      sql.append(" s_dist_04 varchar(24),");
      sql.append(" s_dist_05 varchar(24),");
      sql.append(" s_dist_06 varchar(24),");
      sql.append(" s_dist_07 varchar(24),");
      sql.append(" s_dist_08 varchar(24),");
      sql.append(" s_dist_09 varchar(24),");
      sql.append(" s_dist_10 varchar(24),");
      sql.append(" s_ytd decimal,");
      sql.append(" s_order_cnt int,");
      sql.append(" s_remote_cnt int,");
      sql.append(" s_data varchar(50),");
      sql.append(" PRIMARY KEY(s_w_id, s_i_id) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createDistrictTable() {
    String tableName = "district";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table district (");
      sql.append(" d_id int not null,");
      sql.append(" d_w_id int not null,");
      sql.append(" d_name varchar(10),");
      sql.append(" d_street_1 varchar(20),");
      sql.append(" d_street_2 varchar(20),");
      sql.append(" d_city varchar(20),");
      sql.append(" d_state varchar(2),");
      sql.append(" d_zip varchar(9),");
      sql.append(" d_tax decimal,");
      sql.append(" d_ytd decimal,");
      sql.append(" d_next_o_id int,");
      sql.append(" primary key (d_w_id, d_id) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createCustomerTable() {
    String tableName = "customer";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table customer (");
      sql.append(" c_id int not null,");
      sql.append(" c_d_id int not null,");
      sql.append(" c_w_id int not null,");
      sql.append(" c_first varchar(16),");
      sql.append(" c_middle varchar(2),");
      sql.append(" c_last varchar(16),");
      sql.append(" c_street_1 varchar(20),");
      sql.append(" c_street_2 varchar(20),");
      sql.append(" c_city varchar(20),");
      sql.append(" c_state varchar(2),");
      sql.append(" c_zip varchar(9),");
      sql.append(" c_phone varchar(16),");
      sql.append(" c_since datetime,");
      sql.append(" c_credit varchar(2),");
      sql.append(" c_credit_lim int,");
      sql.append(" c_discount decimal,");
      sql.append(" c_balance decimal,");
      sql.append(" c_ytd_payment decimal,");
      sql.append(" c_payment_cnt int,");
      sql.append(" c_delivery_cnt int,");
      sql.append(" c_data varchar(500),");
      sql.append(" PRIMARY KEY(c_w_id, c_d_id, c_id) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createHistoryTable() {
    String tableName = "history";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table history (");
      sql.append(" h_c_id int,");
      sql.append(" h_c_d_id int,");
      sql.append(" h_c_w_id int,");
      sql.append(" h_d_id int,");
      sql.append(" h_w_id int,");
      sql.append(" h_date datetime,");
      sql.append(" h_amount decimal,");
      sql.append(" h_data varchar(24) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createOrderTable() {
    String tableName = "orders";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table orders (");
      sql.append(" o_id int not null,");
      sql.append(" o_d_id int not null,");
      sql.append(" o_w_id int not null,");
      sql.append(" o_c_id int,");
      sql.append(" o_entry_d datetime,");
      sql.append(" o_carrier_id int,");
      sql.append(" o_ol_cnt int,");
      sql.append(" o_all_local int,");
      sql.append(" PRIMARY KEY(o_w_id, o_d_id, o_id) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createNewOrderTable() {
    String tableName = "new_orders";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table new_orders (");
      sql.append(" no_o_id int not null,");
      sql.append(" no_d_id int not null,");
      sql.append(" no_w_id int not null,");
      sql.append(" PRIMARY KEY(no_w_id, no_d_id, no_o_id))");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createOrderLineTable() {
    String tableName = "order_line";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table order_line (");
      sql.append(" ol_o_id int not null,");
      sql.append(" ol_d_id int not null,");
      sql.append(" ol_w_id int not null,");
      sql.append(" ol_number int not null,");
      sql.append(" ol_i_id int,");
      sql.append(" ol_supply_w_id int,");
      sql.append(" ol_delivery_d datetime,");
      sql.append(" ol_quantity int,");
      sql.append(" ol_amount decimal,");
      sql.append(" ol_dist_info varchar(24),");
      sql.append(" PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createSupplierTable() {
    String tableName = "supplier";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table supplier (");
      sql.append(" su_suppkey int not null,");
      sql.append(" su_name varchar(25),");
      sql.append(" su_address varchar(41),");
      sql.append(" su_nationkey int,");
      sql.append(" su_phone varchar(34),");
      sql.append(" su_acctbal double,");
      sql.append(" su_comment varchar(500),");
      sql.append(" PRIMARY KEY(su_suppkey) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createNationTable() {
    String tableName = "nation";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table nation (");
      sql.append(" n_nationkey int not null,");
      sql.append(" n_name varchar(25),");
      sql.append(" n_regionkey int,");
      sql.append(" n_comment varchar(152),");
      sql.append(" PRIMARY KEY(n_nationkey) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status createRegionTable() {
    String tableName = "region";
    try {
      StringBuilder sql = new StringBuilder();

      Statement stmt = conns.get(0).createStatement();

      if (overwrite) {
        dropTable(tableName);
      }

      sql.append("create table region (");
      sql.append(" r_regionkey int not null,");
      sql.append(" r_name varchar(25),");
      sql.append(" r_comment varchar(152),");
      sql.append(" PRIMARY KEY(r_regionkey) )");

      stmt.execute(sql.toString());

      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public void insertItemBatch(List<Item> batch) {
    LOGGER.info("insertItemBatch: called with {} items", batch.size());
    for (Item i : batch) {
      ObjectNode record = i.asNode();
      String id = itemTable.getDocumentId(record);
      insert("item", record, batch.size());
    }
  }

  @Override
  public void insertWarehouseBatch(List<Warehouse> batch) {
    LOGGER.info("insertWarehouseBatch: called with {} items", batch.size());
    for (Warehouse i : batch) {
      ObjectNode record = i.asNode();
      String id = warehouseTable.getDocumentId(record);
      insert("warehouse", record, batch.size());
    }
  }

  @Override
  public void insertStockBatch(List<Stock> batch) {
    LOGGER.info("insertStockBatch: called with {} items", batch.size());
    for (Stock i : batch) {
      ObjectNode record = i.asNode();
      String id = stockTable.getDocumentId(record);
      insert("stock", record, batch.size());
    }
  }

  @Override
  public void insertDistrictBatch(List<District> batch) {
    LOGGER.info("insertDistrictBatch: called with {} items", batch.size());
    for (District i : batch) {
      ObjectNode record = i.asNode();
      String id = districtTable.getDocumentId(record);
      insert("district", record, batch.size());
    }
  }

  @Override
  public void insertCustomerBatch(List<Customer> batch) {
    LOGGER.info("insertCustomerBatch: called with {} items", batch.size());
    for (Customer i : batch) {
      ObjectNode record = i.asNode();
      String id = customerTable.getDocumentId(record);
      insert("customer", record, batch.size());
    }
  }

  @Override
  public void insertHistoryBatch(List<History> batch) {
    LOGGER.info("insertHistoryBatch: called with {} items", batch.size());
    for (History i : batch) {
      ObjectNode record = i.asNode();
      String id = historyTable.getDocumentId(record);
      insert("history", record, batch.size());
    }
  }

  @Override
  public void insertOrderBatch(List<Order> batch) {
    LOGGER.info("insertOrderBatch: called with {} items", batch.size());
    for (Order i : batch) {
      ObjectNode record = i.asNode();
      String id = orderTable.getDocumentId(record);
      insert("orders", record, batch.size());
    }
  }

  @Override
  public void insertNewOrderBatch(List<NewOrder> batch) {
    LOGGER.info("insertNewOrderBatch: called with {} items", batch.size());
    for (NewOrder i : batch) {
      ObjectNode record = i.asNode();
      String id = newOrderTable.getDocumentId(record);
      insert("new_orders", record, batch.size());
    }
  }

  @Override
  public void insertOrderLineBatch(List<OrderLine> batch) {
    LOGGER.info("insertOrderLineBatch: called with {} items", batch.size());
    for (OrderLine i : batch) {
      ObjectNode record = i.asNode();
      String id = orderLineTable.getDocumentId(record);
      insert("order_line", record, batch.size());
    }
  }

  @Override
  public void insertSupplierBatch(List<Supplier> batch) {
    LOGGER.info("insertSupplierBatch: called with {} items", batch.size());
    for (Supplier i : batch) {
      ObjectNode record = i.asNode();
      String id = supplierTable.getDocumentId(record);
      insert("supplier", record, batch.size());
    }
  }

  @Override
  public void insertNationBatch(List<Nation> batch) {
    LOGGER.info("insertNationBatch: called with {} items", batch.size());
    for (Nation i : batch) {
      ObjectNode record = i.asNode();
      String id = nationTable.getDocumentId(record);
      insert("nation", record, batch.size());
    }
  }

  @Override
  public void insertRegionBatch(List<Region> batch) {
    LOGGER.info("insertRegionBatch: called with {} items", batch.size());
    for (Region i : batch) {
      ObjectNode record = i.asNode();
      String id = regionTable.getDocumentId(record);
      insert("region", record, batch.size());
    }
  }

  /**
   * Ordered field information for insert and update statements.
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }

  private static class OrderedJsonInfo {
    private List<String> columns;
    private List<Object> values;

    OrderedJsonInfo(List<String> columns, List<Object> values) {
      this.columns = columns;
      this.values = values;
    }

    String columnCsv() {
      return String.join(",", columns);
    }

    int numColumns() {
      return columns.size();
    }

    List<Object> getFieldValues() {
      return values;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        LOGGER.error("Invalid {} specified: {}", key, valueStr);
        throw new RuntimeException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    URL propFile;

    if (initialized) {
      LOGGER.debug("Client connection already initialized.");
      return;
    }

    if ((propFile = classloader.getResource(PROPERTY_FILE)) != null
        || (propFile = classloader.getResource(PROPERTY_TEST)) != null) {
      try {
        props.load(propFile.openStream());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    props.putAll(getProperties());

    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String warehouse = props.getProperty(CONNECTION_WAREHOUSE, DEFAULT_PROP);
    String database = props.getProperty(CONNECTION_DB, "bench");
    String schema = props.getProperty(CONNECTION_SCHEMA, "public");
    String driver = props.getProperty(DRIVER_CLASS);

    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);

    debug = props.getProperty("snowflake.debug", "false").equals("true");
    overwrite = props.getProperty("snowflake.dropIfExists", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    Properties prop = new Properties();
    prop.put("user", user);
    prop.put("password", passwd);
    prop.put("db", database);
    prop.put("schema", schema);
    prop.put("warehouse", warehouse);

    try {
      if (driver != null) {
        sqlansiScans = true;
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      final String[] urlArr = urls.split(";;");
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url);
        Connection conn = DriverManager.getConnection(url, prop);

        conn.setAutoCommit(autoCommit);

        shardCount++;
        conns.add(conn);
      }

      this.dbFlavor = DBFlavor.benchDriver();
    } catch (ClassNotFoundException e) {
      LOGGER.error("Error in initializing the JDBC driver: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (SQLException e) {
      LOGGER.error("Error in database operation: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (NumberFormatException e) {
      LOGGER.error("Invalid value for fieldcount property. {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    initialized = true;
  }

  @Override
  public void cleanup() {
    try {
      for (Map.Entry<String, PreparedStatement> entry : stmtMap.entrySet()) {
        PreparedStatement st = entry.getValue();
        if (!st.getConnection().isClosed() && !st.isClosed()) {
          st.executeBatch();
        }
      }
    } catch (SQLException e) {
      LOGGER.error("Error in cleanup execution. {}", e.getMessage(), e);
      e.printStackTrace(System.err);
      throw new RuntimeException(e);
    }

    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      LOGGER.error("Error in closing the connection. {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  private static <T>T retryBlock(Callable<T> block) throws Exception {
    int retryCount = 10;
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      try {
        return block.call();
      } catch (Exception e) {
        LOGGER.debug("Retry count: {} error: {}", retryCount, e.getMessage(), e);
        if (retryNumber == retryCount) {
          throw e;
        } else {
          double factor = waitFactor * retryNumber;
          long wait = (long) factor;
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    return block.call();
  }

  private Connection getDefaultConnection() {
    return conns.get(0);
  }

  public String createInsertSQL(String tableName, String fields, int length) {
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(tableName);
    insert.append(" (");
    insert.append(fields);
    insert.append(") ");
    insert.append("VALUES(?");
    for (int i = 1; i < length; i++) {
      insert.append(",?");
    }
    insert.append(")");
    return insert.toString();
  }

  private PreparedStatement createQueryStatement(String statement) throws SQLException {
    return getDefaultConnection().prepareStatement(statement);
  }

  public void insert(String tableName, ObjectNode record, int batchSize) {
    try {
      retryBlock(() -> {
        List<String> fieldInfo = getFieldNames(record);
        int numFields = fieldInfo.size();
        PreparedStatement insertStatement;

        if (stmtMap.containsKey(tableName)) {
          insertStatement = stmtMap.get(tableName);
        } else {
          insertStatement = createQueryStatement(createInsertSQL(tableName, String.join(",", fieldInfo), numFields));
          stmtMap.put(tableName, insertStatement);
        }

        if (!countMap.containsKey(tableName)) {
          countMap.put(tableName, new AtomicInteger(0));
        }

        for (int i = 1; i <= numFields; i++) {
          String field = fieldInfo.get(i - 1);
          JsonNode value = record.get(field);
          if (value.isNull()) {
            insertStatement.setNull(i, Types.NULL);
          } else if (Objects.equals(field, "c_since") || Objects.equals(field, "h_date") || Objects.equals(field, "ol_delivery_d")) {
            String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
            SimpleDateFormat timeStampFormat = new SimpleDateFormat(DATE_FORMAT);
            Date date = timeStampFormat.parse(value.asText());
            java.sql.Date sqlDate = new java.sql.Date(date.getTime());
            insertStatement.setDate(i, sqlDate);
          } else if (value.isTextual()) {
            insertStatement.setString(i, value.asText());
          } else if (value.isLong()) {
            insertStatement.setLong(i, value.asLong());
          } else if (value.isDouble()) {
            insertStatement.setDouble(i, value.asDouble());
          } else if (value.isFloat()) {
            insertStatement.setDouble(i, value.asDouble());
          } else if (value.isInt()) {
            insertStatement.setInt(i, value.asInt());
          } else if (value.isBoolean()) {
            insertStatement.setBoolean(i, value.asBoolean());
          } else if (value.isBinary()) {
            insertStatement.setString(i, value.asText());
          } else {
            insertStatement.setString(i, value.asText());
          }
        }

        if (debug) {
          LOGGER.debug("Insert statement: {}", insertStatement.toString());
        }

        insertStatement.addBatch();
        countMap.get(tableName).incrementAndGet();

        if (countMap.get(tableName).get() % batchSize == 0) {
          int[] results = insertStatement.executeBatch();
          countMap.get(tableName).set(0);
          for (int r : results) {
            if (r == Statement.EXECUTE_FAILED) {
              LOGGER.error("Batch insert error: table: {} record:\n{}", tableName, record.toPrettyString());
              return null;
            }
          }

          if (!autoCommit) {
            getDefaultConnection().commit();
          }
        }
        return null;
      });
    } catch (Throwable t) {
      LOGGER.error("Error processing insert to table {}: {}", tableName, t.getMessage(), t);
    }
  }

  private OrderedJsonInfo getFieldInfo(ObjectNode record) {
    List<String> columns = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    if (debug) {
      LOGGER.debug("record: \n{}", record.toPrettyString());
    }
    for (Iterator<Map.Entry<String, JsonNode>> it = record.fields(); it.hasNext(); ) {
      try {
        String field = it.next().getKey();
        JsonNode value = record.get(field);
        columns.add(field);
        if (value.isNull()) {
          values.add(null);
        } else if (Objects.equals(field, "c_since") || Objects.equals(field, "h_date") || Objects.equals(field, "ol_delivery_d")) {
          String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
          SimpleDateFormat timeStampFormat = new SimpleDateFormat(DATE_FORMAT);
          Date date = timeStampFormat.parse(value.asText());
          java.sql.Date sqlDate = new java.sql.Date(date.getTime());
          values.add(sqlDate);
        } else {
          if (value.isTextual()) {
            values.add(value.asText());
          } else if (value.isLong()) {
            values.add(value.asLong());
          } else if (value.isDouble()) {
            values.add(value.asDouble());
          } else if (value.isFloat()) {
            values.add(value.asDouble());
          } else if (value.isInt()) {
            values.add(value.asInt());
          } else if (value.isBoolean()) {
            values.add(value.asBoolean());
          } else if (value.isBinary()) {
            values.add(value.asText());
          } else {
            values.add(value.asText());
          }
        }
      } catch (Exception e) {
        LOGGER.error("record processing error: {}", e.getMessage(), e);
      }
    }
    if (debug) {
      LOGGER.debug("{} => {}", String.join(",", columns), values.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
    return new OrderedJsonInfo(columns, values);
  }

  private List<String> getFieldNames(ObjectNode record) {
    List<String> fields = new ArrayList<>();
    record.fieldNames().forEachRemaining(fields::add);
    return fields;
  }
}
