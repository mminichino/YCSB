/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package site.ycsb.db.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Cassandra CQL client.
 * See {@code cassandra2/README.md} for details.
 *
 * @author mminichino
 */
public class CassandraCQLClient extends DB {

  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.cassandra.CassandraCQLClient");

  private static volatile CqlSession session = null;
  private static String tableName;

  private static final ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> scanStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> updateStmts = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> readAllStmt = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> scanAllStmt = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> deleteStmt = new AtomicReference<>();

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String HOSTS_PROPERTY = "cassandra.hosts";
  public static final String PORT_PROPERTY = "cassandra.port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";

  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "cassandra.connecttimeoutmillis";
  public static final String DEFAULT_CONNECT_TIMEOUT_MILLIS = "5000";
  public static final String REQUEST_TIMEOUT_MILLIS_PROPERTY = "cassandra.readtimeoutmillis";
  public static final String DEFAULT_REQUEST_TIMEOUT_MILLIS = "10000";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  public static final String TABLE_NAME_PROPERTY = "cassandra.tablename";
  public static final String TABLE_NAME_PROPERTY_DEFAULT = "usertable";

  public static final String DATACENTER_PROPERTY = "cassandra.datacenter";
  public static final String DATACENTER_PROPERTY_DEFAULT = "dc1";

  public static final String DEBUG_PROPERTY = "cassandra.debug";
  public static final String DEBUG_PROPERTY_DEFAULT = "false";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = false;
  
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (session != null) {
        return;
      }

      try {
        LOGGER.info("Initializing CQL session");

        debug = Boolean.parseBoolean(getProperties().getProperty(DEBUG_PROPERTY, DEBUG_PROPERTY_DEFAULT));

        if (debug) {
          LOGGER.setLevel(Level.DEBUG);
        }

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClient",
              HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);
        Collection<InetSocketAddress> hostList = new LinkedList<>();
        for (String name : hosts) {
          hostList.add(new InetSocketAddress(name, Integer.parseInt(port)));
        }

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);
        tableName = getProperties().getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);
        String datacenter = getProperties().getProperty(DATACENTER_PROPERTY, DATACENTER_PROPERTY_DEFAULT);

        String readConsistency = getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY);
        if (readConsistency != null) {
          readConsistencyLevel = getConsistencyLevel(readConsistency);
        }
        String writeConsistency = getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY);
        if (writeConsistency != null) {
          writeConsistencyLevel = getConsistencyLevel(writeConsistency);
        }

        boolean useSSL = Boolean.parseBoolean(getProperties().getProperty(USE_SSL_CONNECTION, DEFAULT_USE_SSL_CONNECTION));

        CqlSessionBuilder builder = CqlSession.builder();
        if ((username != null) && !username.isEmpty()) {
          builder = builder
              .withAuthCredentials(username, password)
              .addContactPoints(hostList);
          if (useSSL) {
            TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                  public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                  public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                  public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                }
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
            ProgrammaticSslEngineFactory sslEngineFactory = new ProgrammaticSslEngineFactory(sslContext);

            builder = builder.withSslEngineFactory(sslEngineFactory);
          }
        } else {
          builder = builder.addContactPoints(hostList);
        }
        builder.withLocalDatacenter(datacenter);

        ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder();

        loaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4);
        loaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 4);
        loaderBuilder.withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, Duration.ofMillis(4000));

        String connectTimoutMillis = getProperties().getProperty(CONNECT_TIMEOUT_MILLIS_PROPERTY, DEFAULT_CONNECT_TIMEOUT_MILLIS);
        loaderBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(Long.parseLong(connectTimoutMillis)));

        String readTimoutMillis = getProperties().getProperty(REQUEST_TIMEOUT_MILLIS_PROPERTY, DEFAULT_REQUEST_TIMEOUT_MILLIS);
        loaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(Long.parseLong(readTimoutMillis)));

        DriverConfigLoader loader = loaderBuilder.build();
        builder.withConfigLoader(loader);
        builder.withKeyspace(CqlIdentifier.fromCql(keyspace));

        session = builder.build();

        Metadata metadata = session.getMetadata();
        LOGGER.info("Connected to cluster: {}", metadata.getClusterName().orElse(""));

        for (Node discoveredHost : metadata.getNodes().values()) {
          LOGGER.info("Datacenter: {}; Host: {}; Rack: {}",
              discoveredHost.getDatacenter(), discoveredHost.getEndPoint(), discoveredHost.getRack());
        }

      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized
  }

  public ConsistencyLevel getConsistencyLevel(String consistencyLevel) {
    switch (consistencyLevel.toUpperCase()) {
      case "QUORUM":
        return ConsistencyLevel.QUORUM;
      case "LOCAL_ONE":
        return ConsistencyLevel.LOCAL_ONE;
      case "LOCAL_QUORUM":
        return ConsistencyLevel.LOCAL_QUORUM;
      default:
        return ConsistencyLevel.ONE;
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount == 1) {
        LOGGER.info("Closing CQL session");
        session.close();
        session = null;
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      PreparedStatement readStatement = (fields == null) ? readAllStmt.get() : readStmts.get(fields);
      if (readStatement == null) {
        Select query;

        if (fields == null) {
          query = selectFrom(tableName).all().whereColumn(YCSB_KEY).isEqualTo(bindMarker());
        } else {
          query = selectFrom(tableName).columns(fields).whereColumn(YCSB_KEY).isEqualTo(bindMarker());
        }

        readStatement = session.prepare(query.build().setConsistencyLevel(readConsistencyLevel));
        if (fields == null) {
          readAllStmt.getAndSet(readStatement);
        } else {
          readStmts.putIfAbsent(fields, readStatement);
        }
      }

      LOGGER.debug(readStatement.getQuery());
      LOGGER.debug("read key = {}", key);

      ResultSet rs;
      try {
        rs = session.execute(readStatement.bind(key));
      } catch (Throwable t) {
        LOGGER.error("read exception: {}", t.getMessage(), t);
        return Status.ERROR;
      }

      if (rs.getAvailableWithoutFetching() == 0) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ColumnDefinitions cd = Objects.requireNonNull(row).getColumnDefinitions();

      for (ColumnDefinition def : cd) {
        String field = def.getName().toString();
        if (field.equals(YCSB_KEY)) continue;
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(field, new ByteArrayByteIterator(val.array()));
        } else {
          result.put(field, null);
        }
      }

      LOGGER.debug("{} read {} results", key, result.size());

      return Status.OK;

    } catch (Exception e) {
      LOGGER.error("Error reading key: {}: {}", key, e.getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {
      PreparedStatement scanStatement = (fields == null) ? scanAllStmt.get() : scanStmts.get(fields);
      if (scanStatement == null) {
        Select query;

        if (fields == null) {
          query = selectFrom(tableName).all().whereColumn(YCSB_KEY).isGreaterThanOrEqualTo(bindMarker()).limit(bindMarker()).allowFiltering();
        } else {
          query = selectFrom(tableName).columns(fields).whereColumn(YCSB_KEY).isGreaterThanOrEqualTo(bindMarker()).limit(bindMarker()).allowFiltering();
        }

        scanStatement = session.prepare(query.build().setConsistencyLevel(readConsistencyLevel));
        if (fields == null) {
          scanAllStmt.getAndSet(scanStatement);
        } else {
          scanStmts.putIfAbsent(fields, scanStatement);
        }
      }

      LOGGER.debug(scanStatement.getQuery());
      LOGGER.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      ResultSet rs = session.execute(scanStatement.bind(startkey, recordcount));

      for (Row row : rs) {
        HashMap<String, ByteIterator> tuple = new HashMap<>();

        ColumnDefinitions cd = Objects.requireNonNull(row).getColumnDefinitions();

        for (ColumnDefinition def : cd) {
          String field = def.getName().toString();
          if (field.equals(YCSB_KEY)) continue;
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(field, new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(field, null);
          }
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (Exception e) {
      LOGGER.error("Error scanning with startkey: {}: {}", startkey, e.getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement updateStatement = updateStmts.get(fields);

      // Prepare statement on demand
      if (updateStatement == null) {
        List<Assignment> assignments = new ArrayList<>();
        // Add fields
        for (String field : fields) {
          assignments.add(Assignment.setColumn(field, bindMarker()));
        }

        // Add key
        Update update = QueryBuilder.update(tableName).set(assignments).whereColumn(YCSB_KEY).isEqualTo(bindMarker());

        updateStatement = session.prepare(update.build().setConsistencyLevel(writeConsistencyLevel));
        updateStmts.putIfAbsent(new HashSet<>(fields), updateStatement);
      }

      if (debug) {
        LOGGER.debug(updateStatement.getQuery());
        LOGGER.debug("Update key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOGGER.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      BoundStatementBuilder boundStmt = updateStatement.boundStatementBuilder();
      for (String field : fields) {
        boundStmt.setString(field, values.get(field).toString());
      }

      // Add key
      boundStmt.setString(YCSB_KEY, key);

      try {
        session.execute(boundStmt.build());
      } catch (Throwable t) {
        LOGGER.error("update exception: {}", t.getMessage(), t);
        return Status.ERROR;
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error updating key: {}: {}", key, e.getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement insertStatement = insertStmts.get(fields);

      // Prepare statement on demand
      if (insertStatement == null) {
        Map<String, Term> assignments = new HashMap<>();
        assignments.put(YCSB_KEY, bindMarker());
        // Add fields
        for (String field : fields) {
          assignments.put(field, bindMarker());
        }

        // Add key
        Insert insert = QueryBuilder.insertInto(tableName).values(assignments);

        insertStatement = session.prepare(insert.build().setConsistencyLevel(writeConsistencyLevel));
        insertStmts.putIfAbsent(new HashSet<>(fields), insertStatement);
      }

      if (debug) {
        LOGGER.debug(insertStatement.getQuery());
        LOGGER.debug("Insert key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOGGER.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      BoundStatementBuilder boundStmt = insertStatement.boundStatementBuilder();
      boundStmt.setString(YCSB_KEY, key);
      for (String field : fields) {
        boundStmt.setString(field, values.get(field).toString());
      }

      session.execute(boundStmt.build());

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error inserting key: {}: {}", key, e.getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    try {
      PreparedStatement deleteStatement = deleteStmt.get();
      // Prepare statement on demand
      if (deleteStatement == null) {
        Delete query = deleteFrom(tableName).whereColumn(YCSB_KEY).isEqualTo(bindMarker());
        deleteStatement = session.prepare(query.build());
        deleteStmt.getAndSet(deleteStatement);
      }

      LOGGER.debug(deleteStatement.getQuery());
      LOGGER.debug("delete key = {}", key);

      session.execute(deleteStatement.bind(key));

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Error deleting key: {}: {}", key, e.getMessage(), e);
    }

    return Status.ERROR;
  }

}
