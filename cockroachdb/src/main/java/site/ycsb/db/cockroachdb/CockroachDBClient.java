package site.ycsb.db.cockroachdb;

import okhttp3.ConnectionPool;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import site.ycsb.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

import static org.jooq.impl.DSL.*;
import org.jooq.*;
import org.jooq.impl.*;

import javax.sql.PooledConnection;

public class CockroachDBClient extends DB {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.cockroachdb.CockroachDBClient");

  private final ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
  private final ConcurrentMap<Set<String>, PreparedStatement> scanStmts = new ConcurrentHashMap<>();
  private final ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<>();
  private final ConcurrentMap<Set<String>, PreparedStatement> updateStmts = new ConcurrentHashMap<>();
  private final AtomicReference<PreparedStatement> readAllStmt = new AtomicReference<>();
  private final AtomicReference<PreparedStatement> scanAllStmt = new AtomicReference<>();
  private final AtomicReference<PreparedStatement> deleteStmt = new AtomicReference<>();

  public static final String CONNECTION_URL = "db.url";
  public static final String CONNECTION_USER = "db.user";
  public static final String CONNECTION_PASSWD = "db.passwd";
  public static final String CONNECTION_SSL = "db.ssl";

  public static final String YCSB_KEY = "id";

  private static final int MAX_RETRY_COUNT = 3;

  private final Random rand = new Random();
  private Connection connection;
  private final DSLContext create = DSL.using(SQLDialect.POSTGRES);

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String url = props.getProperty(CONNECTION_URL);
    String user = props.getProperty(CONNECTION_USER);
    String passwd = props.getProperty(CONNECTION_PASSWD);
    boolean ssl = Boolean.parseBoolean(props.getProperty(CONNECTION_SSL, "false"));

    try {
      PGSimpleDataSource ds = new PGSimpleDataSource();
      ds.setUrl(url);
      ds.setUser(user);
      ds.setPassword(passwd);
      ds.setSsl(ssl);
      connection = ds.getConnection();
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  public PreparedStatement getInsertStatement(String table, Set<String> fields) throws SQLException {
    PreparedStatement insertStatement = insertStmts.get(fields);
    if (insertStatement == null) {
      InsertQuery<?> insert = create.insertQuery(table(table));
      insert.addValue(field(YCSB_KEY), val(1));
      int index = 2;
      for (String field : fields) {
        insert.addValue(field(field), val(index++));
      }
      PreparedStatement stmt = connection.prepareStatement(insert.getSQL());
      insertStmts.putIfAbsent(new HashSet<>(fields), stmt);
      return stmt;
    } else {
      return insertStatement;
    }
  }

  public PreparedStatement getUpdateStatement(String table, Set<String> fields) throws SQLException {
    PreparedStatement updateStatement = updateStmts.get(fields);
    if (updateStatement == null) {
      UpdateQuery<?> update = create.updateQuery(table(table));
      update.addConditions(field(YCSB_KEY).eq(val(1)));
      int index = 2;
      for (String field : fields) {
        update.addValue(field(field), val(index++));
      }
      PreparedStatement stmt = connection.prepareStatement(update.getSQL());
      updateStmts.putIfAbsent(new HashSet<>(fields), stmt);
      return stmt;
    } else {
      return updateStatement;
    }
  }

  public PreparedStatement getReadStatement(String table, Set<String> fields) throws SQLException {
    PreparedStatement readStatement = (fields == null) ? readAllStmt.get() : readStmts.get(fields);
    if (readStatement == null) {
      SelectQuery<?> select = create.selectQuery(table(table));
      select.addConditions(field(YCSB_KEY).eq(val(1)));
      if (fields != null) {
        for (String field : fields) {
          select.addSelect(field(field));
        }
      } else {
        select.addSelect(asterisk());
      }
      PreparedStatement stmt = connection.prepareStatement(select.getSQL());
      if (fields == null) {
        readAllStmt.getAndSet(stmt);
      } else {
        readStmts.putIfAbsent(fields, stmt);
      }
      return stmt;
    } else {
      return readStatement;
    }
  }

  public PreparedStatement getScanStatement(String table, Set<String> fields) throws SQLException {
    PreparedStatement scanStatement = (fields == null) ? scanAllStmt.get() : scanStmts.get(fields);
    if (scanStatement == null) {
      SelectQuery<?> select = create.selectQuery(table(table));
      select.addConditions(field(YCSB_KEY).ge(val(1)));
      select.addLimit(val(2));
      if (fields != null) {
        for (String field : fields) {
          select.addSelect(field(field));
        }
      } else {
        select.addSelect(asterisk());
      }
      PreparedStatement stmt = connection.prepareStatement(select.getSQL());
      if (fields == null) {
        scanAllStmt.getAndSet(stmt);
      } else {
        scanStmts.putIfAbsent(fields, stmt);
      }
      return stmt;
    } else {
      return scanStatement;
    }
  }

  public PreparedStatement getDeleteStatement(String table) throws SQLException {
    PreparedStatement deleteStatement = deleteStmt.get();
    if (deleteStatement == null) {
      DeleteQuery<?> delete = create.deleteQuery(table(table));
      delete.addConditions(field(YCSB_KEY).eq(val(1)));
      PreparedStatement stmt = connection.prepareStatement(delete.getSQL());
      deleteStmt.getAndSet(stmt);
      return stmt;
    } else {
      return deleteStatement;
    }
  }

  public ResultSet runSQL(PreparedStatement pstmt, String... args) throws DBException {
    int retryCount = 0;

    while (retryCount <= MAX_RETRY_COUNT) {
      try {
        for (int i=0; i<args.length; i++) {
          int place = i + 1;
          String arg = args[i];
          try {
            int val = Integer.parseInt(arg);
            pstmt.setInt(place, val);
          } catch (NumberFormatException e) {
            pstmt.setString(place, arg);
          }
        }
        pstmt.execute();
        connection.commit();
        return pstmt.getResultSet();
      } catch (SQLException e) {
        LOGGER.debug("sql exception occurred: sql state = [{}] message = [{}] retry counter = {}", e.getSQLState(), e.getMessage(), retryCount);
        retryCount++;
        int sleepMillis = (int)(Math.pow(2, retryCount) * 100) + rand.nextInt(100);
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ignored) {}
      }
    }
    LOGGER.error("Query failed: {}", pstmt.toString());
    throw new DBException("hit max of " + MAX_RETRY_COUNT + " retries, aborting");
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      ResultSet rs = runSQL(getReadStatement(table, fields), key);
      rs.next();
      ResultSetMetaData meta = rs.getMetaData();
      int colCount = meta.getColumnCount();
      for (int i=1; i <= colCount; i++) {
        String field = meta.getColumnName(i);
        byte[] val = rs.getBytes(i);
        if (val != null) {
          result.put(field, new ByteArrayByteIterator(val));
        } else {
          result.put(field, null);
        }
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error reading key: {}: {}", key, e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      ResultSet rs = runSQL(getScanStatement(table, fields), startkey, String.valueOf(recordcount));
      ResultSetMetaData meta = rs.getMetaData();
      int colCount = meta.getColumnCount();
      while (rs.next()) {
        HashMap<String, ByteIterator> tuple = new HashMap<>();
        for (int i=1; i <= colCount; i++) {
          String field = meta.getColumnName(i);
          byte[] val = rs.getBytes(i);
          if (val != null) {
            tuple.put(field, new ByteArrayByteIterator(val));
          } else {
            tuple.put(field, null);
          }
        }
        result.add(tuple);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error scanning from key: {}: {}", startkey, e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Set<String> fields = values.keySet();
    String[] parameters = new String[fields.size() + 1];
    try {
      parameters[0] = key;
      int index = 1;
      for (String field : fields) {
        parameters[index++] = values.get(field).toString();
      }
      runSQL(getUpdateStatement(table, fields), parameters);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error updating key: {}: {}", key, e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Set<String> fields = values.keySet();
    String[] parameters = new String[fields.size() + 1];
    try {
      parameters[0] = key;
      int index = 1;
      for (String field : fields) {
        parameters[index++] = values.get(field).toString();
      }
      runSQL(getInsertStatement(table, fields), parameters);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error updating key: {}: {}", key, e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      runSQL(getDeleteStatement(table), key);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error deleting key: {}: {}", key, e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
