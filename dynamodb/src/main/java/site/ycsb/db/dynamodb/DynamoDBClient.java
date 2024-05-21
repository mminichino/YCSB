/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2016 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package site.ycsb.db.dynamodb;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;
import ch.qos.logback.classic.Level;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * DynamoDB client for YCSB.
 */

public class DynamoDBClient extends DB {

  /**
   * Defines the primary key type used in this particular DB instance.
   * <p>
   * By default, the primary key type is "HASH". Optionally, the user can
   * choose to use hash_and_range key type. See documentation in the
   * DynamoDB.Properties file for more details.
   */
  private enum PrimaryKeyType {
    HASH,
    HASH_AND_RANGE
  }

  private static final Object INIT_COORDINATOR = new Object();

  private static volatile DynamoDbClient dynamoDB;
  private static String primaryKeyName;
  private static PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;

  // If the user choose to use HASH_AND_RANGE as primary key type, then
  // the following two variables become relevant. See documentation in the
  // DynamoDB.Properties file for more details.
  private static String hashKeyValue;
  private static String hashKeyName;

  private boolean consistentRead = false;
  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.dynamodb.DynamoDBClient");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";

  @Override
  public void init() throws DBException {
    boolean debug = getProperties().getProperty("dynamodb.debug", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String endpoint = getProperties().getProperty("dynamodb.endpoint", null);
    primaryKeyName = getProperties().getProperty("dynamodb.primaryKey", "userid");
    String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", "HASH");
    consistentRead = getProperties().getProperty("dynamodb.consistentReads", "false").equals("true");
    String configuredRegion = getProperties().getProperty("dynamodb.region", "us-east-1");
    String accessKeyID = getProperties().getProperty("dynamodb.accessKeyID",
        System.getenv("AWS_ACCESS_KEY_ID"));
    String secretAccessKey = getProperties().getProperty("dynamodb.secretAccessKey",
        System.getenv("AWS_SECRET_ACCESS_KEY"));
    String sessionToken = getProperties().getProperty("dynamodb.sessionToken",
        System.getenv("AWS_SESSION_TOKEN"));

    try {
      primaryKeyType = PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new DBException("Invalid primary key mode specified: " + primaryKeyTypeString +
          ". Expecting HASH or HASH_AND_RANGE.");
    }

    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // When the primary key type is HASH_AND_RANGE, keys used by YCSB
      // are range keys, so we can benchmark performance of individual hash
      // partitions. In this case, the user must specify the hash key's name
      // and optionally can designate a value for the hash key.

      String configuredHashKeyName = getProperties().getProperty("dynamodb.hashKeyName", null);
      if (null == configuredHashKeyName || configuredHashKeyName.isEmpty()) {
        throw new DBException("Must specify a non-empty hash key name when the primary key type is HASH_AND_RANGE.");
      }
      hashKeyName = configuredHashKeyName;
      hashKeyValue = getProperties().getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
    }

    Region awsRegion = Region.of(configuredRegion);

    synchronized (INIT_COORDINATOR) {
      if (dynamoDB == null) {
        AwsCredentials credentials;
        try {
          if (sessionToken == null) {
            credentials = AwsBasicCredentials.create(accessKeyID, secretAccessKey);
          } else {
            credentials = AwsSessionCredentials.create(accessKeyID, secretAccessKey, sessionToken);
          }
          StaticCredentialsProvider provider = StaticCredentialsProvider.create(credentials);
          DynamoDbClientBuilder dynamoDBBuilder = DynamoDbClient.builder()
              .defaultsMode(DefaultsMode.AUTO)
              .region(awsRegion)
              .credentialsProvider(provider);
          if (endpoint != null) {
            dynamoDBBuilder.endpointOverride(URI.create(endpoint));
          }

          dynamoDB = dynamoDBBuilder.build();
          LOGGER.debug("dynamodb connection successful");
        } catch (Exception e1) {
          LOGGER.error("DynamoDBClient.init(): Could not initialize DynamoDB client.", e1);
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("readkey: {} from table: {}", key, table);
    }

    GetItemRequest req = GetItemRequest.builder()
        .key(createPrimaryKey(key))
        .tableName(table)
        .consistentRead(consistentRead)
        .build();

    try {
      Map<String,AttributeValue> res = dynamoDB.getItem(req).item();
      if (null != res) {
        result.putAll(extractResult(res));
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Result: {}", res);
        }
      }
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("read error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("scan {} records from key: {} on table: {}", recordcount, startkey, table);
    }

    /*
     * on DynamoDB's scan, startkey is *exclusive* so we need to
     * getItem(startKey) and then use scan for the res
    */
    GetItemRequest req = GetItemRequest.builder()
        .key(createPrimaryKey(startkey))
        .tableName(table)
        .consistentRead(consistentRead)
        .build();

    try {
      Map<String,AttributeValue> res = dynamoDB.getItem(req).item();
      if (null != res) {
        result.add(extractResult(res));
      }
    } catch (Throwable ex) {
      LOGGER.error("scan error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }

    if (recordcount > 1) {
      recordcount--;
    } else {
      return Status.OK;
    }

    ScanRequest scanRequest = ScanRequest.builder()
        .tableName(table)
        .exclusiveStartKey(createPrimaryKey(startkey))
        .limit(recordcount)
        .consistentRead(consistentRead)
        .build();

    try {
      ScanResponse response = dynamoDB.scan(scanRequest);
      for (Map<String, AttributeValue> item : response.items()) {
        result.add(extractResult(item));
      }
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("scan error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: {} from table: {}", key, table);
    }

    UpdateItemRequest req = UpdateItemRequest.builder()
        .tableName(table)
        .key(createPrimaryKey(key))
        .attributeUpdates(createAttributeUpdate(values))
        .build();

    try {
      dynamoDB.updateItem(req);
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("update error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: {}-{} from table: {}", primaryKeyName, key, table);
    }

    PutItemRequest req = PutItemRequest.builder()
        .tableName(table)
        .item(createAttributes(key, values))
        .build();

    try {
      dynamoDB.putItem(req);
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("insert error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: {} from table: {}", key, table);
    }

    DeleteItemRequest req = DeleteItemRequest.builder()
        .tableName(table)
        .key(createPrimaryKey(key))
        .build();

    try {
      dynamoDB.deleteItem(req);
      return Status.OK;
    } catch (Throwable ex) {
      LOGGER.error("delete error: {}", ex.getMessage(), ex);
      return Status.ERROR;
    }
  }

  private static Map<String, AttributeValue> createAttributes(String key, Map<String, ByteIterator> values) {
    HashMap<String, AttributeValue> attributes = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      attributes.put(entry.getKey(), AttributeValue.builder().s(entry.getValue().toString()).build());
    }
    attributes.put(primaryKeyName, AttributeValue.builder().s(key).build());
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      attributes.put(hashKeyName, AttributeValue.builder().s(hashKeyValue).build());
    }
    return attributes;
  }

  private static Map<String, AttributeValueUpdate> createAttributeUpdate(Map<String, ByteIterator> values) {
    HashMap<String, AttributeValueUpdate> attributes = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      attributes.put(entry.getKey(), AttributeValueUpdate.builder()
          .value(AttributeValue.builder().s(entry.getValue().toString()).build())
          .action(AttributeAction.PUT)
          .build());
    }
    return attributes;
  }

  private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.size());

    Set<String> keys = item.keySet();
    for (String key : keys) {
      rItems.put(key, new StringByteIterator(item.get(key).toString()));
    }
    return rItems;
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    HashMap<String,AttributeValue> keyToGet = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      keyToGet.put(primaryKeyName, AttributeValue.builder().s(key).build());
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      keyToGet.put(hashKeyName, AttributeValue.builder().s(hashKeyValue).build());
      keyToGet.put(primaryKeyName, AttributeValue.builder().s(key).build());
    }
    return keyToGet;
  }
}
