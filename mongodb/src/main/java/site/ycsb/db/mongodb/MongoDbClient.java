/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 */
package site.ycsb.db.mongodb;

import com.mongodb.*;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import site.ycsb.db.mongodb.SubscriberHelpers.ObservableSubscriber;
import site.ycsb.db.mongodb.SubscriberHelpers.OperationSubscriber;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;

import org.slf4j.LoggerFactory;

import org.bson.Document;
import org.bson.types.Binary;

import java.util.*;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB {

  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.mongodb.MongoDbClient");

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = 1;
  private static final Object INIT_COORDINATOR = new Object();

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions().upsert(true);

  /** The database name to access. */
  private static volatile MongoClient mongoClient;
  private static volatile MongoDatabase database;
  private static volatile MongoCollection<Document> collection;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /**
   * Cleanup any state for this DB.
   */
  @Override
  public void cleanup() {
  }

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      Document query = new Document("_id", key);
      ObservableSubscriber<DeleteResult> subscriber = new OperationSubscriber<>();
      collection.deleteOne(query).subscribe(subscriber);
      subscriber.await();
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("delete exception: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private WriteConcern setDurabilityLevel(final int value) {
    switch(value){
      case 1:
        return WriteConcern.JOURNALED;
      case 2:
        return WriteConcern.W1;
      case 3:
        return WriteConcern.MAJORITY;
      default :
        return WriteConcern.ACKNOWLEDGED;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() {
    synchronized (INIT_COORDINATOR) {
      if (mongoClient == null) {
        Properties props = getProperties();

        // Set is inserts are done as upserts. Defaults to false.
        useUpsert = Boolean.parseBoolean(
            props.getProperty("mongodb.upsert", "true"));

        WriteConcern writeConcern = setDurabilityLevel(
            Integer.parseInt(props.getProperty("mongodb.durability", "0")));

        String databaseName = props.getProperty("mongodb.database", "ycsb");
        String table = props.getProperty("mongodb.collection", "usertable");

        String url = props.getProperty("mongodb.url", null);

        if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
          LOGGER.error("ERROR: Invalid URL: '{}'. Must be of the form " +
              "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' or " +
              "'mongodb+srv://<host>/database?options'. " +
              "http://docs.mongodb.org/manual/reference/connection-string/", url);
        }

        ServerApi serverApi = ServerApi.builder()
            .version(ServerApiVersion.V1)
            .build();

        MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(url))
            .serverApi(serverApi)
            .build();

        mongoClient = MongoClients.create(settings);
        try {
          database = mongoClient.getDatabase(databaseName);
          collection = database.getCollection(table).withWriteConcern(writeConcern);
          LOGGER.debug("mongo client connection created with {}", url);
        } catch (Exception e) {
          LOGGER.error("client init error: {}", e.getMessage(), e);
        }
      }
    }
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
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      if (useUpsert) {
        Document query = new Document("_id", key);
        Document fieldsToSet = new Document();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          fieldsToSet.put(entry.getKey(), entry.getValue().toString());
        }
        Document update = new Document("$set", fieldsToSet);
        ObservableSubscriber<UpdateResult> subscriber = new OperationSubscriber<>();
        collection.updateOne(query, update, UPDATE_WITH_UPSERT).subscribe(subscriber);
        subscriber.await();
      } else {
        Document toInsert = new Document("_id", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          toInsert.put(entry.getKey(), entry.getValue().toString());
        }
        ObservableSubscriber<InsertOneResult> subscriber = new OperationSubscriber<>();
        collection.insertOne(toInsert).subscribe(subscriber);
        subscriber.await();
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("insert error: {}", e.getMessage(), e);
      return Status.ERROR;
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
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      collection = database.getCollection(table);
      Document query = new Document("_id", key);

      ObservableSubscriber<Document> subscriber = new OperationSubscriber<>();
      collection.find(query).subscribe(subscriber);
      Document queryResult = subscriber.first();

      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Throwable e) {
      LOGGER.error("read error: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
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
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);

      ObservableSubscriber<Document> subscriber = new OperationSubscriber<>();
      collection.find(query).limit(recordcount).subscribe(subscriber);
      List<Document> docs = subscriber.get();

      if (docs.isEmpty()) {
        LOGGER.error("Nothing found in scan for key {}", startkey);
        return Status.ERROR;
      }

      for (Document obj : docs) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<>();

        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("scan error: {}", e.getMessage(), e);
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
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toString());
      }
      Document update = new Document("$set", fieldsToSet);

      ObservableSubscriber<UpdateResult> subscriber = new OperationSubscriber<>();
      collection.updateOne(query, update, UPDATE_WITH_UPSERT).subscribe(subscriber);
      subscriber.await();
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("update error: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   * 
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }
}
