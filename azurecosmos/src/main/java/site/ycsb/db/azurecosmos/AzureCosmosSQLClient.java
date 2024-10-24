/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package site.ycsb.db.azurecosmos;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Azure Cosmos DB Java SDK 4.59.0 client for YCSB.
 */

public class AzureCosmosSQLClient extends DB {

  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.azurecosmos.AzureCosmosClient");
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Object INIT_COORDINATOR = new Object();

  // Default configuration values
  private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.SESSION;
  private static final String DEFAULT_DATABASE_NAME = "ycsb";
  private static final boolean DEFAULT_USE_GATEWAY = false;
  private static final boolean DEFAULT_USE_UPSERT = true;
  private static final int DEFAULT_MAX_DEGREE_OF_PARALLELISM = -1;
  private static final int DEFAULT_MAX_BUFFERED_ITEM_COUNT = 0;
  private static final int DEFAULT_PREFERRED_PAGE_SIZE = -1;
  public static final int NUM_UPDATE_ATTEMPTS = 4;
  private static final String DEFAULT_USER_AGENT = "azurecosmos-ycsb";
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";
  public static final int FIELD_COUNT_PROPERTY_DEFAULT = 10;
  private static CosmosClient client;
  private static CosmosDatabase database;
  private static String databaseName;
  private static boolean useUpsert;
  private static int maxDegreeOfParallelism;
  private static int maxBufferedItemCount;
  private static int preferredPageSize;
  private static Map<String, CosmosContainer> containerCache;
  private static String userAgent;
  private static String allFields;
  private static final boolean enableDebug = false;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final SimpleModule serializer = new SimpleModule("ByteIteratorSerializer")
      .addSerializer(ByteIterator.class, new ByteIteratorSerializer());
  private static final SimpleModule deserializer = new SimpleModule("ByteIteratorDeserializer")
      .addDeserializer(ByteIterator.class, new ByteIteratorDeserializer());

  @Override
  public void init() throws DBException {
    synchronized (INIT_COORDINATOR) {
      try {
        if (database == null) {
          initAzureCosmosClient();
        }
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  private void initAzureCosmosClient() throws DBException {
    mapper.registerModule(serializer);
    mapper.registerModule(deserializer);

    // Connection properties
    String primaryKey = this.getStringProperty("azurecosmos.primaryKey", null);
    if (primaryKey == null || primaryKey.isEmpty()) {
      throw new DBException("Missing primary key required to connect to the database.");
    }

    String uri = this.getStringProperty("azurecosmos.uri", null);

    AzureCosmosSQLClient.userAgent = this.getStringProperty("azurecosmos.userAgent", DEFAULT_USER_AGENT);

    AzureCosmosSQLClient.useUpsert = this.getBooleanProperty("azurecosmos.useUpsert", DEFAULT_USE_UPSERT);

    AzureCosmosSQLClient.databaseName = this.getStringProperty("azurecosmos.databaseName", DEFAULT_DATABASE_NAME);

    AzureCosmosSQLClient.maxDegreeOfParallelism = this.getIntProperty("azurecosmos.maxDegreeOfParallelism",
        DEFAULT_MAX_DEGREE_OF_PARALLELISM);

    AzureCosmosSQLClient.maxBufferedItemCount = this.getIntProperty("azurecosmos.maxBufferedItemCount",
        DEFAULT_MAX_BUFFERED_ITEM_COUNT);

    AzureCosmosSQLClient.preferredPageSize = this.getIntProperty("azurecosmos.preferredPageSize",
        DEFAULT_PREFERRED_PAGE_SIZE);

    ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(
        this.getStringProperty("azurecosmos.consistencyLevel", DEFAULT_CONSISTENCY_LEVEL.toString().toUpperCase()));
    boolean useGateway = this.getBooleanProperty("azurecosmos.useGateway", DEFAULT_USE_GATEWAY);

    int fieldCount = this.getIntProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT);

    ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
    int maxRetryAttemptsOnThrottledRequests = this.getIntProperty("azurecosmos.maxRetryAttemptsOnThrottledRequests",
        -1);
    if (maxRetryAttemptsOnThrottledRequests != -1) {
      retryOptions.setMaxRetryAttemptsOnThrottledRequests(maxRetryAttemptsOnThrottledRequests);
    }

    // Direct connection config options.
    DirectConnectionConfig directConnectionConfig = new DirectConnectionConfig();
    int directMaxConnectionsPerEndpoint = this.getIntProperty("azurecosmos.directMaxConnectionsPerEndpoint", -1);
    if (directMaxConnectionsPerEndpoint != -1) {
      directConnectionConfig.setMaxConnectionsPerEndpoint(directMaxConnectionsPerEndpoint);
    }

    int directIdleConnectionTimeoutInSeconds = this.getIntProperty("azurecosmos.directIdleConnectionTimeoutInSeconds",
        -1);
    if (directIdleConnectionTimeoutInSeconds != -1) {
      directConnectionConfig.setIdleConnectionTimeout(Duration.ofSeconds(directIdleConnectionTimeoutInSeconds));
    }

    // Gateway connection config options.
    GatewayConnectionConfig gatewayConnectionConfig = new GatewayConnectionConfig();

    int gatewayMaxConnectionPoolSize = this.getIntProperty("azurecosmos.gatewayMaxConnectionPoolSize", -1);
    if (gatewayMaxConnectionPoolSize != -1) {
      gatewayConnectionConfig.setMaxConnectionPoolSize(gatewayMaxConnectionPoolSize);
    }

    int gatewayIdleConnectionTimeoutInSeconds = this.getIntProperty("azurecosmos.gatewayIdleConnectionTimeoutInSeconds",
        -1);
    if (gatewayIdleConnectionTimeoutInSeconds != -1) {
      gatewayConnectionConfig.setIdleConnectionTimeout(Duration.ofSeconds(gatewayIdleConnectionTimeoutInSeconds));
    }

    StringBuilder fieldBuilder = new StringBuilder();
    fieldBuilder.append("c.field0");
    for (int idx = 1; idx < fieldCount; idx++) {
      fieldBuilder.append(",c.field");
      fieldBuilder.append(idx);
    }
    allFields = fieldBuilder.toString();

    try {
      LOGGER.debug(
          "Creating Cosmos DB client {}, useGateway={}, consistencyLevel={},"
              + " maxRetryAttemptsOnThrottledRequests={}, maxRetryWaitTimeInSeconds={}"
              + " useUpsert={}, maxDegreeOfParallelism={}, maxBufferedItemCount={}, preferredPageSize={}",
          uri, useGateway, consistencyLevel, retryOptions.getMaxRetryAttemptsOnThrottledRequests(),
          retryOptions.getMaxRetryWaitTime().toMillis() / 1000, AzureCosmosSQLClient.useUpsert,
          AzureCosmosSQLClient.maxDegreeOfParallelism, AzureCosmosSQLClient.maxBufferedItemCount,
          AzureCosmosSQLClient.preferredPageSize);

      CosmosClientBuilder builder = new CosmosClientBuilder().endpoint(uri).key(primaryKey)
          .throttlingRetryOptions(retryOptions).consistencyLevel(consistencyLevel).userAgentSuffix(userAgent);

      if (useGateway) {
        builder = builder.gatewayMode(gatewayConnectionConfig);
      } else {
        builder = builder.directMode(directConnectionConfig);
      }

      AzureCosmosSQLClient.client = builder.buildClient();
      LOGGER.debug("Azure Cosmos DB connection created to {}", uri);
    } catch (IllegalArgumentException e) {
      throw new DBException("Illegal argument passed in. Check the format of your parameters.", e);
    }

    AzureCosmosSQLClient.containerCache = new ConcurrentHashMap<>();

    // Verify the database exists
    try {
      AzureCosmosSQLClient.database = AzureCosmosSQLClient.client.getDatabase(databaseName);
      AzureCosmosSQLClient.database.read();
    } catch (CosmosException e) {
      throw new DBException(
          "Invalid database name (" + AzureCosmosSQLClient.databaseName + ") or failed to read database.", e);
    }
  }

  private String getStringProperty(String propertyName, String defaultValue) {
    return getProperties().getProperty(propertyName, defaultValue);
  }

  private boolean getBooleanProperty(String propertyName, boolean defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(stringVal);
  }

  private int getIntProperty(String propertyName, int defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(stringVal);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static <T>T retryBlock(Callable<T> block) throws Exception {
    int retryCount = 10;
    long waitFactor = 100L;
    for (int retryNumber = 1; retryNumber <= retryCount; retryNumber++) {
      try {
        return block.call();
      } catch (Exception e) {
        if (retryNumber == retryCount) {
          LOGGER.error(String.format("Retry count %d: %s: error: %s", retryNumber, e.getClass(), e.getMessage()));
          Writer buffer = new StringWriter();
          PrintWriter pw = new PrintWriter(buffer);
          e.printStackTrace(pw);
          LOGGER.error(String.format("%s", buffer));
          throw e;
        } else {
          double factor = waitFactor * Math.pow(2, retryNumber);
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

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public synchronized void cleanup() {
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      result = retryBlock(() -> {
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setMaxDegreeOfParallelism(AzureCosmosSQLClient.maxDegreeOfParallelism);
        queryOptions.setMaxBufferedItemCount(AzureCosmosSQLClient.maxBufferedItemCount);

        CosmosContainer container = AzureCosmosSQLClient.containerCache.get(table);
        if (container == null) {
          container = AzureCosmosSQLClient.database.getContainer(table);
          AzureCosmosSQLClient.containerCache.put(table, container);
        }

        List<SqlParameter> paramList = new ArrayList<>();
        paramList.add(new SqlParameter("@key", key));

        SqlQuerySpec querySpec = new SqlQuerySpec("SELECT " + allFields + " FROM c WHERE c.id = @key", paramList);

        CosmosPagedIterable<ObjectNode> pagedIterable = container.queryItems(querySpec, queryOptions, ObjectNode.class);
        Iterator<FeedResponse<ObjectNode>> pageIterator = pagedIterable
            .iterableByPage(AzureCosmosSQLClient.preferredPageSize).iterator();

        ObjectNode pageDocs = pageIterator.next().getResults().get(0);

        TypeReference<Map<String, ByteIterator>> typeRef  = new TypeReference<>() {};
        return mapper.convertValue(pageDocs, typeRef);
      });
      if (enableDebug) {
        LOGGER.debug("Doc: {}", result.toString());
      }
      return Status.OK;
    } catch (Throwable e) {
      LOGGER.error("Failed to read key {} in collection {} in database {}", key, table, AzureCosmosSQLClient.databaseName, e);
      return Status.NOT_FOUND;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set
   *                    field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try {
      return retryBlock(() -> {
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setMaxDegreeOfParallelism(AzureCosmosSQLClient.maxDegreeOfParallelism);
        queryOptions.setMaxBufferedItemCount(AzureCosmosSQLClient.maxBufferedItemCount);

        CosmosContainer container = AzureCosmosSQLClient.containerCache.get(table);
        if (container == null) {
          container = AzureCosmosSQLClient.database.getContainer(table);
          AzureCosmosSQLClient.containerCache.put(table, container);
        }

        List<SqlParameter> paramList = new ArrayList<>();
        paramList.add(new SqlParameter("@startkey", startkey));

        SqlQuerySpec querySpec = new SqlQuerySpec(
            this.createSelectTop(fields, recordcount) + " FROM root r WHERE r.id >= @startkey", paramList);
        CosmosPagedIterable<ObjectNode> pagedIterable = container.queryItems(querySpec, queryOptions, ObjectNode.class);

        for (FeedResponse<ObjectNode> objectNodeFeedResponse : pagedIterable.iterableByPage(AzureCosmosSQLClient.preferredPageSize)) {
          List<ObjectNode> pageDocs = objectNodeFeedResponse.getResults();
          for (ObjectNode doc : pageDocs) {
            TypeReference<HashMap<String, ByteIterator>> typeRef = new TypeReference<>() {};
            HashMap<String, ByteIterator> r = mapper.convertValue(doc, typeRef);
            result.add(r);
          }
        }
        return Status.OK;
      });
    } catch (Throwable e) {
      LOGGER.error("Failed to query key {} from collection {} in database {}", startkey, table,
          AzureCosmosSQLClient.databaseName, e);
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record key,
   * overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    //    String readEtag = "";

    // Azure Cosmos DB does not have patch support. Until then, we need to read
    // the document, update it, and then write it back.
    // This could be made more efficient by using a stored procedure
    // and doing the read/modify write on the server side. Perhaps
    // that will be a future improvement.
    for (int attempt = 0; attempt < NUM_UPDATE_ATTEMPTS; attempt++) {
      try {
        return retryBlock(() -> {
            CosmosContainer container = AzureCosmosSQLClient.containerCache.get(table);
            if (container == null) {
              container = AzureCosmosSQLClient.database.getContainer(table);
              AzureCosmosSQLClient.containerCache.put(table, container);
            }

            CosmosItemResponse<ObjectNode> response = container.readItem(key, new PartitionKey(key), ObjectNode.class);
            final String readEtag = response.getETag();
            ObjectNode node = response.getItem();

            for (Entry<String, ByteIterator> pair : values.entrySet()) {
              node.put(pair.getKey(), pair.getValue().toString());
            }

            CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
            requestOptions.setIfMatchETag(readEtag);
            PartitionKey pk = new PartitionKey(key);
            if (AzureCosmosSQLClient.useUpsert) {
              container.upsertItem(node, pk, new CosmosItemRequestOptions());
            } else {
              container.replaceItem(node, key, pk, requestOptions);
            }

            return Status.OK;
          });
      } catch (Throwable e) {
        LOGGER.error("Failed to update key {} to collection {} in database {} on attempt {}", key, table,
            AzureCosmosSQLClient.databaseName, attempt, e);
      }
    }

    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Insert key: {} into table: {}", key, table);
    }

    try {
      return retryBlock(() -> {
          CosmosContainer container = AzureCosmosSQLClient.containerCache.get(table);
          if (container == null) {
            container = AzureCosmosSQLClient.database.getContainer(table);
            AzureCosmosSQLClient.containerCache.put(table, container);
          }
          PartitionKey pk = new PartitionKey(key);
          ObjectNode node = OBJECT_MAPPER.createObjectNode();

          node.put("id", key);
          for (Entry<String, ByteIterator> pair : values.entrySet()) {
            node.put(pair.getKey(), pair.getValue().toString());
          }
          if (AzureCosmosSQLClient.useUpsert) {
            container.upsertItem(node, pk, new CosmosItemRequestOptions());
          } else {
            container.createItem(node, pk, new CosmosItemRequestOptions());
          }
          return Status.OK;
        });
    } catch (Throwable e) {
      LOGGER.error("Failed to insert key {} to collection {} in database {}", key, table,
          AzureCosmosSQLClient.databaseName, e);
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Delete key {} from table {}", key, table);
    }
    try {
      return retryBlock(() -> {
          CosmosContainer container = AzureCosmosSQLClient.containerCache.get(table);
          if (container == null) {
            container = AzureCosmosSQLClient.database.getContainer(table);
            AzureCosmosSQLClient.containerCache.put(table, container);
          }
          container.deleteItem(key, new PartitionKey(key), new CosmosItemRequestOptions());

          return Status.OK;
        });
    } catch (Exception e) {
      LOGGER.error("Failed to delete key {} in collection {}", key, table, e);
    }
    return Status.ERROR;
  }

  private String createSelectTop(Set<String> fields, int top) {
    if (fields == null) {
      return "SELECT TOP " + top + " * ";
    } else {
      StringBuilder result = new StringBuilder("SELECT TOP ").append(top).append(" ");
      int initLength = result.length();
      for (String field : fields) {
        if (result.length() != initLength) {
          result.append(", ");
        }
        result.append("r['").append(field).append("'] ");
      }
      return result.toString();
    }
  }
}
