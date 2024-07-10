package site.ycsb.db.snowflake;

import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;
import site.ycsb.*;
import site.ycsb.db.snowflake.flavors.DBFlavor;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SnowflakeTPCRun extends BenchRun {

  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.snowflake.SnowflakeTPCRun");

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
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  private boolean autoCommit;
  private boolean batchUpdates;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;

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

  private Connection getDefaultConnection() {
    return conns.get(0);
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
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
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String warehouse = props.getProperty(CONNECTION_WAREHOUSE, DEFAULT_PROP);
    String database = props.getProperty(CONNECTION_DB, "ycsb");
    String schema = props.getProperty(CONNECTION_SCHEMA, "public");
    String driver = props.getProperty(DRIVER_CLASS);

    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    this.batchSize = getIntProperty(props, DB_BATCH_SIZE);

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    this.batchUpdates = getBoolProperty(props, JDBC_BATCH_UPDATES, false);

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

      System.out.println("Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

      this.dbFlavor = DBFlavor.defaultDriver();
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBC driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }

    initialized = true;
  }

  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        e.printStackTrace(System.err);
        throw new DBException(e);
      }
    }

    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  public static Class<?> toClass(int type) {
    Class<?> result = Object.class;

    switch (type) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        result = String.class;
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        result = java.math.BigDecimal.class;
        break;
      case Types.BIT:
        result = Boolean.class;
        break;
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        result = Integer.class;
        break;
      case Types.BIGINT:
        result = Long.class;
        break;
      case Types.REAL:
      case Types.FLOAT:
      case Types.DOUBLE:
        result = Double.class;
        break;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        result = Byte[].class;
        break;
      case Types.DATE:
        result = java.sql.Date.class;
        break;
      case Types.TIME:
        result = java.sql.Time.class;
        break;
      case Types.TIMESTAMP:
        result = java.sql.Timestamp.class;
        break;
    }

    return result;
  }

  @Override
  public List<ObjectNode> query(String statement) {
    List<ObjectNode> results = new ArrayList<>();
    try {
      PreparedStatement readStatement = createQueryStatement(statement);
      ResultSet resultSet = readStatement.executeQuery();
      while (resultSet.next()) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode data = mapper.createObjectNode();
        ResultSetMetaData metaData = resultSet.getMetaData();
        LOGGER.info(String.valueOf(metaData.getColumnCount()));
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          String columnName = metaData.getColumnName(i);
          JsonNode value = mapper.valueToTree(resultSet.getObject(i));
          data.set(columnName, value);
        }
        results.add(data);
      }
      resultSet.close();
      return results;
    } catch (Exception e) {
      LOGGER.error("Error processing query: {}", e.getMessage(), e);
      return null;
    }
  }

  private PreparedStatement createQueryStatement(String statement) throws SQLException {
    return getDefaultConnection().prepareStatement(statement);
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
}
