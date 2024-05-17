/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 * updated by MongoDB 3/18/2015
 * updated by Michael Minichino 5/17/24
 *
 */

/*
 * MongoDB client binding for YCSB.
 */
package site.ycsb.db.mongodb;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import org.jetbrains.annotations.NotNull;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.Status;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.UuidRepresentation;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;

class UuidUtils {

  public static byte[] asBytes(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}

/**
 * MongoDB client for YCSB framework.
 * -
 * Properties to set:
 * -
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb mongodb.writeConcern=acknowledged
 * For replica set use:
 * mongodb.url=mongodb://hostname:27017?replicaSet=nameOfYourReplSet
 * to pass connection to multiple mongos end points to round-robin between them, separate
 * hostnames with "|" character
 *
 * @author ypai
 */
public class MongoDbClient extends DB {

  /** Logger */
  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.mongodb.MongoDbClient");

  /** Used to include a field in a response. */
  protected static final Integer INCLUDE = 1;

  /** A singleton MongoClient instance. */
  private static MongoClient[] mongo;

  private static MongoDatabase[] db;

  private static int serverCounter = 0;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The default read preference for the test */
  private static ReadPreference readPreference;

  /** Allow inserting batches to save time during load */
  private static Integer BATCHSIZE;
  private List<Document> insertList = null;
  private Integer insertCount = 0;

  /** The database to access. */
  private static String database;

  /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
  private static final AtomicInteger initCount = new AtomicInteger(0);

  /** Measure of how compressible the data is, compressibility=10 means the data can compress tenfold.
   *  The default is 1, which is incompressible */
  private static float compressibility = (float) 1.0;

  private static String datatype = "binData";

  private static final String algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";

  private static boolean isSharded = false;

  enum Encryption {
    UNENCRYPTED,
    FLE,
    QUERYABLE,
  }

  private static Encryption encryptionType = Encryption.UNENCRYPTED;
  private static ArrayList<Long> contentionFactors;
  private static HashMap<String, DiscreteGenerator> discreteFields;

  private static String generateSchema(String keyId, int numFields) {
    StringBuilder schema = new StringBuilder();

    schema.append(
        "{" +
            "  properties: {" );

    for(int i =0; i < numFields; i++) {
      schema.append("    field").append(i).append(": {")
          .append("      encrypt: {")
          .append("        keyId: [{")
          .append("          \"$binary\": {")
          .append("            \"base64\": \"").append(keyId).append("\",")
          .append("            \"subType\": \"04\"")
          .append("          }")
          .append("        }],")
          .append("        bsonType: \"").append(datatype).append("\",")
          .append("        algorithm: \"").append(algorithm).append("\"")
          .append("      }")
          .append("    },");
    }

    schema.append(
        "  }," +
            "  \"bsonType\": \"object\"" +
            "}");

    return schema.toString();
  }

  private static String generateRemoteSchema(String keyId, int numFields) {
    return "{ $jsonSchema : " + generateSchema(keyId, numFields) + "}";
  }

  private static BsonDocument generateEncryptedFieldsDocument(MongoCollection<BsonDocument> keyCollection,
                                                              ClientEncryption clientEncryption, int numFields) {
    ArrayList<BsonDocument> fields = new ArrayList<>();
    for (int i = 0; i < numFields; i++) {
      UUID dataKeyId = getDataKeyOrCreateUUID(keyCollection, clientEncryption);
      BsonDocument queries = new BsonDocument("queryType", new BsonString("equality"));

      if (i < contentionFactors.size() && contentionFactors.get(i) > -1) {
        queries.append("contention", new BsonInt64(contentionFactors.get(i)));
      }
      fields.add(new BsonDocument("path", new BsonString("field" + i))
          .append("keyId", new BsonBinary(dataKeyId))
          .append("bsonType", new BsonString(datatype))
          .append("queries", new BsonArray(List.of(queries))));
    }
    return new BsonDocument("fields", new BsonArray(fields));
  }

  private static synchronized UUID getDataKeyOrCreateUUID(MongoCollection<BsonDocument> keyCollection,
                                                          ClientEncryption clientEncryption) {
    BsonDocument findFilter = new BsonDocument();
    BsonDocument keyDoc = keyCollection.find(findFilter).first();
    if (keyDoc == null) {
      BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
      return dataKeyId.asUuid();
    } else {
      return keyDoc.getBinary("_id").asUuid();
    }
  }

  private static synchronized String getDataKeyOrCreate(MongoCollection<BsonDocument> keyCollection,
                                                        ClientEncryption clientEncryption) {
    UUID dataKeyId = getDataKeyOrCreateUUID(keyCollection, clientEncryption);
    return Base64.getEncoder().encodeToString(UuidUtils.asBytes(dataKeyId));
  }

  private static byte[] overrideDataIfDiscrete(String key, byte[] data) {
    // override the data with a value from discrete set
    // generator.nextString() is read-only with a thread-local random number
    // generator, so there's no need to synchronize this function.
    DiscreteGenerator generator = discreteFields.get(key);
    if (generator != null) {
      byte[] discrete = Objects.requireNonNull(generator.nextString()).getBytes();

      if (discrete.length >= data.length) {
        // do not truncate if discrete value is longer than desired length
        return discrete;
      }

      // extend & pad to desired length
      data = Arrays.copyOf(discrete, data.length);
      Arrays.fill(data, discrete.length, data.length, (byte)'x');
    }
    return data;
  }

  private static boolean isCollectionCreated(MongoClient client, String dbName, String collName) {
    for (String c : client.getDatabase(dbName).listCollectionNames()) {
      if (c.equals(collName)) {
        return true;
      }
    }
    return false;
  }

  private static AutoEncryptionSettings generateEncryptionSettings(String url, String collName, Properties props) {
    boolean remote_schema = Boolean.parseBoolean(props.getProperty("mongodb.remote_schema", "false"));
    int numFields = Integer.parseInt(props.getProperty("mongodb.numFleFields", "10"));

    boolean useCryptSharedLib = Boolean.parseBoolean(props.getProperty("mongodb.useCryptSharedLib", "false"));
    String cryptSharedLibPath = "";
    if (useCryptSharedLib) {
      cryptSharedLibPath = props.getProperty("mongodb.cryptSharedLibPath", "");
      if (cryptSharedLibPath.isEmpty()) {
        System.err.println("ERROR: mongodb.cryptSharedLibPath must be non-empty if mongodb.useCryptSharedLib is true");
        System.exit(1);
      }
    }

    Map<String, Map<String, Object>> kmsProviders = getStringMapMap();

    // Use the same database, admin is slow
    String keyVaultNamespace = database + ".datakeys";
    String keyVaultUrls = url;
    if (!keyVaultUrls.startsWith("mongodb")) {
      keyVaultUrls = "mongodb://" + keyVaultUrls;
    }

    ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
        .keyVaultMongoClientSettings(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(keyVaultUrls))
            .readPreference(readPreference)
            .writeConcern(writeConcern)
            .build())
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .build();

    ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

    MongoClientSettings clientSettings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(keyVaultUrls))
        .uuidRepresentation(UuidRepresentation.STANDARD).build();

    MongoClient vaultClient = MongoClients.create(clientSettings);

    final MongoCollection<BsonDocument> keyCollection = vaultClient.getDatabase(database)
        .getCollection(keyVaultNamespace, BsonDocument.class);

    String base64DataKeyId = getDataKeyOrCreate(keyCollection, clientEncryption);

    String collNamespace = database + "." + collName;

    Map<String, Object> extraOptions = new HashMap<>();
    extraOptions.put("mongocryptdBypassSpawn", true);
    if (useCryptSharedLib) {
      extraOptions.put("cryptSharedLibRequired", true);
      extraOptions.put("cryptSharedLibPath", cryptSharedLibPath);
    }

    AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
        .keyVaultNamespace(keyVaultNamespace)
        .extraOptions(extraOptions)
        .kmsProviders(kmsProviders);

    if (encryptionType == Encryption.FLE) {
      autoEncryptionSettingsBuilder.schemaMap(Collections.singletonMap(collNamespace,
          // Need a schema that references the new data key
          BsonDocument.parse(generateSchema(base64DataKeyId, numFields))));
    }
    else if (encryptionType == Encryption.QUERYABLE) {
      MongoClient client = MongoClients.create(clientSettings);

      if (!isCollectionCreated(client, database, collName)) {
        CreateCollectionOptions options = new CreateCollectionOptions();
        BsonDocument encryptedFieldsDocument = generateEncryptedFieldsDocument(keyCollection, clientEncryption, numFields);

        autoEncryptionSettingsBuilder.encryptedFieldsMap(
            Collections.singletonMap(collNamespace, encryptedFieldsDocument));

        options.encryptedFields(encryptedFieldsDocument);

        // This creates the encrypted data collection (EDC) and the auxiliary
        // collections, as well as the index on the __safeContent__ field.
        client.getDatabase(database).createCollection(collName, options);

        if (isSharded) {
          BsonDocument enableShardingCmd = new BsonDocument("enableSharding", new BsonString(database));
          client.getDatabase("admin").runCommand(enableShardingCmd);

          BsonDocument shardCollCmd = new BsonDocument("shardCollection", new BsonString(collNamespace))
              .append("key", new BsonDocument("_id", new BsonString("hashed")));
          client.getDatabase("admin").runCommand(shardCollCmd);
        }
      }
      return autoEncryptionSettingsBuilder.build();
    }

    if (remote_schema) {
      MongoClient client = MongoClients.create(keyVaultUrls);
      CreateCollectionOptions options = new CreateCollectionOptions();
      options.getValidationOptions().validator(BsonDocument.parse(generateRemoteSchema(base64DataKeyId, numFields)));
      try {
        client.getDatabase(database).createCollection(collName,  options);
      } catch (com.mongodb.MongoCommandException e) {
        if (client.getDatabase(database).getCollection(collName).estimatedDocumentCount() <= 0) {
          LOGGER.error("ERROR: Failed to create collection {} with error", collName, e);
        }
      }
    }

    return autoEncryptionSettingsBuilder.build();
  }

  private static @NotNull Map<String, Map<String, Object>> getStringMapMap() {
    byte[] localMasterKey = new byte[]{0x77, 0x1f, 0x2d, 0x7d, 0x76, 0x74, 0x39, 0x08, 0x50, 0x0b, 0x61, 0x14,
        0x3a, 0x07, 0x24, 0x7c, 0x37, 0x7b, 0x60, 0x0f, 0x09, 0x11, 0x23, 0x65,
        0x35, 0x01, 0x3a, 0x76, 0x5f, 0x3e, 0x4b, 0x6a, 0x65, 0x77, 0x21, 0x6d,
        0x34, 0x13, 0x24, 0x1b, 0x47, 0x73, 0x21, 0x5d, 0x56, 0x6a, 0x38, 0x30,
        0x6d, 0x5e, 0x79, 0x1b, 0x25, 0x4d, 0x2a, 0x00, 0x7c, 0x0b, 0x65, 0x1d,
        0x70, 0x22, 0x22, 0x61, 0x2e, 0x6a, 0x52, 0x46, 0x6a, 0x43, 0x43, 0x23,
        0x58, 0x21, 0x78, 0x59, 0x64, 0x35, 0x5c, 0x23, 0x00, 0x27, 0x43, 0x7d,
        0x50, 0x13, 0x65, 0x3c, 0x54, 0x1e, 0x74, 0x3c, 0x3b, 0x57, 0x21, 0x1a};

    return Collections.singletonMap("local", Collections.singletonMap("key", localMasterKey));
  }

  private static ArrayList<Long> parseCommaSeparatedIntegers(String toParse, long defaultValue) {
    if (toParse.trim().isEmpty()) {
      return new ArrayList<>();
    }
    // -1 for limit implies that any trailing empty strings are included in the output array
    String[] values = toParse.split(",", -1);
    ArrayList<Long> parsedValues = new ArrayList<>(Collections.nCopies(values.length, defaultValue));
    for (int i = 0; i < values.length; i++) {
      String v = values[i].trim();
      parsedValues.set(i, v.isEmpty() ? defaultValue : Long.parseLong(v));
    }
    return parsedValues;
  }

  private static HashMap<String, DiscreteGenerator> createDiscreteFieldsMap(String cardinalities) {
    ArrayList<Long> parsedCardinalities = parseCommaSeparatedIntegers(cardinalities, 0);
    HashMap<String, DiscreteGenerator> outputMap = new HashMap<>();
    for (int i = 0; i < parsedCardinalities.size(); i++) {
      Long value = parsedCardinalities.get(i);
      if (value <= 0) {
        continue;
      }
      DiscreteGenerator gen = new DiscreteGenerator();
      for (long j = 0; j < value; j++) {
        gen.addValue(1, "value" + j);
      }
      outputMap.put("field" + i, gen);
    }
    return outputMap;
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() {
    initCount.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongo != null) {
        return;
      }

      // initialize MongoDb driver
      Properties props = getProperties();
      String urls = props.getProperty("mongodb.url", "mongodb://localhost:27017");

      /* Credentials */
      database = props.getProperty("mongodb.database", "ycsb");
      String collection = props.getProperty("mongodb.collection", "usertable");

      // Retrieve username and password from properties, set to empty string if they are undefined
      String username = props.getProperty("mongodb.username", "developer");
      String password = props.getProperty("mongodb.password", "password");

      // If the URI contains an @, that means a username and password are specified here as well, so this will parse them out
      if (urls.contains("@")) {
        String uriCredentials = urls.substring(urls.indexOf("//") + 2, urls.indexOf("@"));
        String[] uriCredentialsList = uriCredentials.split(":");
        username = uriCredentialsList[0];
        password = uriCredentialsList[1];
      }

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      final String batchSizeString = props.getProperty("batchsize", "1");
      BATCHSIZE = Integer.parseInt(batchSizeString);

      // allow "string" in addition to "byte" array for data type
      datatype = props.getProperty("datatype","binData");

      final String compressibilityString = props.getProperty("compressibility", "1");
      compressibility = Float.parseFloat(compressibilityString);

      // Set connection pool to size of ycsb thread pool
      final String maxConnections = props.getProperty("threadcount", "100");

      String writeConcernType = props.getProperty("mongodb.writeConcern",
          "acknowledged").toLowerCase();
      switch (writeConcernType) {
        case "unacknowledged":
          writeConcern = WriteConcern.UNACKNOWLEDGED;
          break;
        case "acknowledged":
          writeConcern = WriteConcern.ACKNOWLEDGED;
          break;
        case "journaled":
          writeConcern = WriteConcern.JOURNALED;
          break;
        case "replica_acknowledged":
          writeConcern = WriteConcern.W2;
          break;
        case "majority":
          writeConcern = WriteConcern.MAJORITY;
          break;
        default:
          System.err.println("ERROR: Invalid writeConcern: '"
              + writeConcernType
              + "'. "
              + "Must be [ unacknowledged | acknowledged | journaled | replica_acknowledged | majority ]");
          System.exit(1);
      }

      // readPreference
      String readPreferenceType = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
      switch (readPreferenceType) {
        case "primary":
          readPreference = ReadPreference.primary();
          break;
        case "primary_preferred":
          readPreference = ReadPreference.primaryPreferred();
          break;
        case "secondary":
          readPreference = ReadPreference.secondary();
          break;
        case "secondary_preferred":
          readPreference = ReadPreference.secondaryPreferred();
          break;
        case "nearest":
          readPreference = ReadPreference.nearest();
          break;
        default:
          System.err.println("ERROR: Invalid readPreference: '"
              + readPreferenceType
              + "'. Must be [ primary | primary_preferred | secondary | secondary_preferred | nearest ]");
          System.exit(1);
      }

      // sharded
      isSharded = Boolean.parseBoolean(props.getProperty("mongodb.sharded", "false"));

      // encryption - FLE or Queryable Encryption
      boolean use_fle = Boolean.parseBoolean(props.getProperty("mongodb.fle", "false"));
      boolean use_qe = Boolean.parseBoolean(props.getProperty("mongodb.qe", "false"));
      boolean use_encryption = use_fle || use_qe;

      if (use_fle && use_qe) {
        System.err.println("ERROR: mongodb.fle and mongodb.qe cannot both be true");
        System.exit(1);
      }
      if (use_fle) {
        encryptionType = Encryption.FLE;
      }
      if (use_qe) {
        encryptionType = Encryption.QUERYABLE;
      }

      contentionFactors = parseCommaSeparatedIntegers(props.getProperty("mongodb.contentionFactors", ""), -1);
      discreteFields = createDiscreteFieldsMap(props.getProperty("mongodb.cardinalities", ""));

      try {
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
        // Need to use a larger connection pool to talk to mongocryptd/keyvault
        if (use_encryption) {
          settingsBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(Integer.parseInt(maxConnections) * 3));
        } else {
          settingsBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(Integer.parseInt(maxConnections)));
        }
        settingsBuilder.writeConcern(writeConcern);
        settingsBuilder.readPreference(readPreference);

        String userPassword = username + ":" + password + "@";

        String[] server = urls.split("\\|"); // split on the "|" character
        mongo = new MongoClient[server.length];
        db = new MongoDatabase[server.length];

        for (int i=0; i<server.length; i++) {
          // If the URI does not contain credentials, but they are provided in the properties, append them to the URI
          String url = server[i].contains("@")
              ? server[i]
              : server[i].replace("://","://" + userPassword);
          if ( i==0 && use_encryption) {
            AutoEncryptionSettings autoEncryptionSettings = generateEncryptionSettings(url, collection, props);
            settingsBuilder.autoEncryptionSettings(autoEncryptionSettings);
          }

          // if mongodb:// prefix is present then this is MongoClientURI format
          // combine with options to get MongoClient
          if (url.startsWith("mongodb://") || url.startsWith("mongodb+srv://") ) {
            settingsBuilder.applyConnectionString(new ConnectionString(url));
            mongo[i] = MongoClients.create(settingsBuilder.build());
            LOGGER.debug("mongo connection created to {}", url);
          } else {
            settingsBuilder.applyToClusterSettings(builder ->
                builder.hosts(Collections.singletonList(new ServerAddress(url))));
            mongo[i] = MongoClients.create(settingsBuilder.build());
            LOGGER.debug("DEBUG mongo server connection to {}", mongo[i].toString());
          }

          db[i] = mongo[i].getDatabase(database);

        }
      } catch (Exception e1) {
        LOGGER.error("Could not initialize MongoDB connection pool for Loader", e1);
      }
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() {
    if (initCount.decrementAndGet() <= 0) {
      for (MongoClient mongoClient : mongo) {
        try {
          mongoClient.close();
        } catch (Exception e1) { /* ignore */ }
      }
    }
  }

  private byte[] applyCompressibility(byte[] data){
    long string_length = data.length;

    long random_string_length = Math.round(string_length /compressibility);
    long compressible_len = string_length - random_string_length;
    for(int i=0;i<compressible_len;i++)
      data[i] = 97;
    return data;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
      Document q = new Document("_id", key);
      collection.deleteMany(q);
      return Status.OK;
    }
    catch (Exception e) {
      LOGGER.error("delete error", e);
      return Status.ERROR;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
                    Map<String, ByteIterator> values) {
    MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
    Document r = new Document("_id", key);
    for (String k : values.keySet()) {
      byte[] data = overrideDataIfDiscrete(k, values.get(k).toArray());
      if (datatype.equals("string")) {
        r.put(k, new String(applyCompressibility(data)));
      } else {
        r.put(k,applyCompressibility(data));
      }
    }
    if (BATCHSIZE == 1 ) {
      try {
        collection.insertOne(r);
        return Status.OK;
      }
      catch (Exception e) {
        LOGGER.error("insert error on key {}", key, e);
        return Status.ERROR;
      }
    }
    if (insertCount == 0) {
      insertList = new ArrayList<>(BATCHSIZE);
    }
    insertCount++;
    insertList.add(r);
    if (insertCount < BATCHSIZE) {
      return Status.OK;
    } else {
      try {
        collection.insertMany(insertList);
        insertCount = 0;
        return Status.OK;
      }
      catch (Exception e) {
        LOGGER.error("bulk insert error on key", e);
        return Status.ERROR;
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Status read(String table, String key, Set<String> fields,
                  Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
      Document q = new Document("_id", key);
      Document fieldsToReturn;

      Document queryResult;
      if (fields != null) {
        fieldsToReturn = new Document();
        for (final String field : fields) {
          fieldsToReturn.put(field, INCLUDE);
        }
        queryResult = collection.find(q).projection(fieldsToReturn).first();
      }
      else {
        queryResult = collection.find(q).first();
      }

      if (queryResult != null) {
        // TODO: this is wrong.  It is totally violating the expected type of the values in result, which is ByteIterator
        // TODO: somewhere up the chain this should be resulting in a ClassCastException
        result.putAll(new LinkedHashMap(queryResult));
        return Status.OK;
      }
      LOGGER.error("No results returned for key {}", key);
      return Status.ERROR;
    }
    catch (Exception e) {
      LOGGER.error("read error", e);
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                    Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
      Document q = new Document("_id", key);
      Document u = new Document();
      Document fieldsToSet = new Document();
      for (String tmpKey : values.keySet()) {
        byte[] data = overrideDataIfDiscrete(tmpKey, values.get(tmpKey).toArray());
        if (datatype.equals("string")) {
          fieldsToSet.put(tmpKey, new String(applyCompressibility(data)));
        } else {
          fieldsToSet.put(tmpKey, applyCompressibility(data));
        }
      }
      u.put("$set", fieldsToSet);
      UpdateResult res = collection.updateOne(q, u);
      if (res.getMatchedCount() == 0) {
        LOGGER.error("Nothing updated for key {}", key);
        return Status.ERROR;
      }
      return Status.OK;
    }
    catch (Exception e) {
      LOGGER.error("update error", e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                  Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = db[serverCounter++%db.length].getCollection(table);
      Document fieldsToReturn = null;
      // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
      Document scanRange = new Document("$gte", startkey);
      Document q = new Document("_id", scanRange);
      Document s = new Document("_id",INCLUDE);
      if (fields != null) {
        fieldsToReturn = new Document();
        for (final String field : fields) {
          fieldsToReturn.put(field, INCLUDE);
        }
      }
      cursor = collection.find(q).projection(fieldsToReturn).sort(s).limit(recordcount).cursor();
      if (!cursor.hasNext()) {
        LOGGER.error("Nothing found in scan for key {}", startkey);
        return Status.ERROR;
      }
      while (cursor.hasNext()) {
        // toMap() returns a Map, but result.add() expects a
        // Map<String,String>. Hence, the suppress warnings.
        HashMap<String, ByteIterator> resultMap = new HashMap<>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    }
    catch (Exception e) {
      LOGGER.error("scan error", e);
      return Status.ERROR;
    }
    finally {
      if( cursor != null ) {
        cursor.close();
      }
    }

  }

  /**
   * TODO - Finish
   *
   * @param resultMap result map
   * @param document source document
   */
  protected void fillMap(HashMap<String, ByteIterator> resultMap, Document document) {
    for (Map.Entry<String, Object> entry : document.entrySet()) {
      if (entry.getValue() instanceof byte[]) {
        resultMap.put(entry.getKey(), new ByteArrayByteIterator(
            (byte[]) entry.getValue()));
      }
    }
  }
}
