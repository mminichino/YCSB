package site.ycsb.db.couchbase3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import org.apache.hadoop.fs.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

/**
 * Clean Cluster after Testing.
 */
public class CouchbaseS3Export {
  protected static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseS3Export");
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_SCOPE = "couchbase.scope";
  public static final String CLUSTER_COLLECTION = "couchbase.collection";
  public static final String CLUSTER_S3_BUCKET = "couchbase.s3Bucket";

  public static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
  public static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
  public static final String AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN";

  public static String awsAccessKey;
  public static String awsSecretKey;
  public static String awsSessionToken;
  public static String awsCredentialsProvider = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

  public static final AtomicLong fileCounter = new AtomicLong(0);
  public static final AtomicLong opCounter = new AtomicLong(0);
  public static final AtomicLong totalCounter = new AtomicLong(0);

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "properties");
    Option collection = new Option("c", "collection", true, "source collection");
    source.setRequired(true);
    collection.setRequired(false);
    options.addOption(source);
    options.addOption(collection);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("ClusterInit", options);
      System.exit(1);
    }

    String propFile = cmd.getOptionValue("properties");
    String collectionName = cmd.hasOption("collection") ? cmd.getOptionValue("collection") : null;

    try {
      properties.load(Files.newInputStream(Paths.get(propFile)));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }

    awsAccessKey = System.getenv(AWS_ACCESS_KEY_ID);
    awsSecretKey = System.getenv(AWS_SECRET_ACCESS_KEY);
    awsSessionToken = System.getenv(AWS_SESSION_TOKEN);

    try {
      doImport(properties, collectionName);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static Schema createSchema(String tableName, JsonNode sample) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = mapper.createObjectNode();
    config.put("type", "record");
    config.put("name", tableName);
    config.put("namespace", "com.example");
    ArrayNode fields = mapper.createArrayNode();
    for (Iterator<Map.Entry<String, JsonNode>> it = sample.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> column = it.next();
      if (column.getValue().isBoolean()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "boolean"));
      } else if (column.getValue().isInt()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "int"));
      } else if (column.getValue().isTextual()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "string"));
      } else if (column.getValue().isDouble()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "double"));
      } else if (column.getValue().isBinary()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "bytes"));
      } else if (column.getValue().isFloat()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "float"));
      } else if (column.getValue().isLong()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "long"));
      } else if (column.getValue().isNull()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "null"));
      }
    }
    config.set("fields", fields);
    return new Schema.Parser().parse(config.toString());
  }

  public static GenericRecord getRecord(Schema schema, JsonNode document) {
    GenericRecord record = new GenericData.Record(schema);
    for (Iterator<Map.Entry<String, JsonNode>> it = document.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> column = it.next();
      if (column.getValue().isBoolean()) {
        record.put(column.getKey(), column.getValue());
      } else if (column.getValue().isInt()) {
        record.put(column.getKey(), Integer.valueOf(column.getValue().asInt()));
      } else if (column.getValue().isTextual()) {
        record.put(column.getKey(), column.getValue());
      } else if (column.getValue().isDouble()) {
        record.put(column.getKey(), Double.valueOf(column.getValue().asDouble()));
      } else if (column.getValue().isBinary()) {
        record.put(column.getKey(), column.getValue());
      } else if (column.getValue().isFloat()) {
        record.put(column.getKey(), Double.valueOf(column.getValue().asDouble()));
      } else if (column.getValue().isLong()) {
        record.put(column.getKey(), Long.valueOf(column.getValue().asLong()));
      } else if (column.getValue().isNull()) {
        record.put(column.getKey(), column.getValue());
      }
    }
    return record;
  }

  public static JsonNode getSample(String host, String user, String password, boolean ssl, String bucket, String scope, String collection) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;
    dbBuilder.connect(host, user, password)
        .ssl(ssl)
        .bucket(bucket)
        .scope(scope)
        .collection(collection);
    db = dbBuilder.build();
    return db.runQuery(String.format("SELECT * from %s LIMIT 1", collection)).get(0).get(collection);
  }

  public static void doImport(Properties properties, String collectionName) {
    String clusterCollection;
    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "bench");
    String clusterScope = properties.getProperty(CLUSTER_SCOPE, "bench");
    if (collectionName == null) {
      clusterCollection = properties.getProperty(CLUSTER_COLLECTION, "bench");
    } else {
      clusterCollection = collectionName;
    }
    String s3Bucket = properties.getProperty(CLUSTER_S3_BUCKET, "bucket");

    System.out.printf("Exporting collection %s to S3 bucket \"%s\"\n", clusterCollection, s3Bucket);

    exportCollection(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket, clusterScope, clusterCollection, s3Bucket);
  }

  private static void exportCollection(String host, String user, String password, boolean ssl, String bucket, String scope, String collection, String s3Bucket) {
    CouchbaseStream stream = new CouchbaseStream(host, user, password, bucket, ssl, scope, collection);
    ObjectMapper mapper = new ObjectMapper();
    System.out.println("Start export");

//    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
//    CouchbaseConnect db;
//    dbBuilder.connect(host, user, password)
//        .ssl(ssl)
//        .bucket(bucket)
//        .scope(scope)
//        .collection(collection);
//    db = dbBuilder.build();
//    db.connectKeyspace();

    JsonNode sample = getSample(host, user, password, ssl, bucket, scope, collection);
    Schema schema = createSchema(collection, sample);
    Configuration config = new Configuration();
    config.set("fs.s3a.access.key", awsAccessKey);
    config.set("fs.s3a.secret.key", awsSecretKey);
    config.set("fs.s3a.session.token", awsSessionToken);
    config.set("fs.s3a.aws.credentials.provider", awsCredentialsProvider);

    System.out.println("Enter try block");
    try {
      stream.streamDocumentContents();
      stream.startToNow();
      System.out.println("Enter while block");
      while (!stream.isAtEnd() || !stream.isDocQueueEmpty()) {
        System.out.println("Start block");
        String document;
        long fileNumber = fileCounter.incrementAndGet();
        Path path = new Path(String.format("s3a://%s/%s/%s-%04d.parquet", s3Bucket, collection, collection, fileNumber));
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
            .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
            .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
            .withSchema(schema)
            .withConf(config)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withValidation(false)
            .withDictionaryEncoding(false)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build();
        while ((opCounter.incrementAndGet() % 100_001) != 0 && (document = stream.getDocQueueEntry()) != null) {
          JsonNode node = mapper.readTree(document);
          writer.write(getRecord(schema, node));
          totalCounter.incrementAndGet();
          if (totalCounter.get() % 1_000 == 0) {
            LOGGER.info(String.format("Processed %,d records", totalCounter.get()));
          }
        }
        opCounter.set(0);
        writer.close();
      }
      stream.stop();
      System.out.printf(" -> Exported %,d records\n", totalCounter.get());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private CouchbaseS3Export() {
    super();
  }
}
