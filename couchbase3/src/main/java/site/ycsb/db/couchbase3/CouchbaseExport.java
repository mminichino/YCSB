package site.ycsb.db.couchbase3;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.dcp.*;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * Couchbase Stream Utility.
 */
public class CouchbaseExport {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseExport");

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
  public static final AtomicLong dataSizeCounter = new AtomicLong(0);
  public static final AtomicLong totalCounter = new AtomicLong(0);

  private static String hostname;
  private static String username;
  private static String password;
  private static String bucket;
  private static String scope;
  private static String collection;
  private static String s3Bucket;
  private static Boolean useSsl;
  private static boolean collectionEnabled;
  private final AtomicLong totalSize = new AtomicLong(0);
  private final AtomicLong docCount = new AtomicLong(0);
  private final AtomicLong sentCount = new AtomicLong(0);
  private final PriorityBlockingQueue<ByteBuffer> queue = new PriorityBlockingQueue<>();
  private static final BlockingQueue<String> docQueue = new LinkedBlockingQueue<>();
  private final AtomicBoolean streamOn = new AtomicBoolean(true);
  private int batchSize;
  private static Client client;

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final Object WRITE_COORDINATOR = new Object();

  private static final AtomicBoolean lastPartition = new AtomicBoolean(false);

  private static Schema schema;
  private static Configuration config;
  private static volatile ParquetWriter<GenericRecord> writer;
  private static Path path;

  public static void main(String[] args) {
    StringBuilder connectBuilder = new StringBuilder();
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "properties");
    Option collectionOpt = new Option("c", "collection", true, "source collection");
    source.setRequired(true);
    collectionOpt.setRequired(false);
    options.addOption(source);
    options.addOption(collectionOpt);

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

    hostname = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    username = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    password = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    useSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    bucket = properties.getProperty(CLUSTER_BUCKET, "bench");
    scope = properties.getProperty(CLUSTER_SCOPE, "bench");
    if (collectionName == null) {
      collection = properties.getProperty(CLUSTER_COLLECTION, "bench");
    } else {
      collection = collectionName;
    }
    s3Bucket = properties.getProperty(CLUSTER_S3_BUCKET, "bucket");

    boolean debug = properties.getProperty("couchbase.debug", "false").equals("true");

    if (debug) {
      LOGGER.setLevel(Level.DEBUG);
    }

    System.out.printf("Exporting collection %s to S3 bucket \"%s\"\n", collection, s3Bucket);

    String couchbasePrefix;
    if (useSsl) {
      couchbasePrefix = "couchbases://";
    } else {
      couchbasePrefix = "couchbase://";
    }

    connectBuilder.append(couchbasePrefix);
    connectBuilder.append(hostname);

    String connectString = connectBuilder.toString();

    collectionEnabled = !collection.equals("_default");

    Consumer<SecurityConfig.Builder> secClientConfig = securityConfig -> {
      securityConfig.enableTls(useSsl)
          .enableHostnameVerification(false)
          .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
    };

    client = Client.builder()
        .connectionString(connectString)
        .bucket(bucket)
        .collectionsAware(true)
        .collectionNames(scope + "." + collection)
        .securityConfig(secClientConfig)
        .credentials(username, password)
        .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 1048576)
        .bufferAckWatermark(75)
        .build();

    client.controlEventHandler((flowController, event) -> {
      flowController.ack(event);
      event.release();
    });

    JsonNode sample = getSample(hostname, username, password, useSsl, bucket, scope, collection);
    schema = createSchema(collection, sample);
    config = new Configuration();
    config.set("fs.s3a.access.key", awsAccessKey);
    config.set("fs.s3a.secret.key", awsSecretKey);
    config.set("fs.s3a.session.token", awsSessionToken);
    config.set("fs.s3a.aws.credentials.provider", awsCredentialsProvider);

    streamDocuments();
    createSystemEventHandler();
    startToNow();

    while (!lastPartition.get() && !client.sessionState().isAtEnd()) {
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        System.out.println("Interrupted");
      }
    }

    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException e) {
      System.out.println("Interrupted");
    }

    synchronized (WRITE_COORDINATOR) {
      while (!docQueue.isEmpty()) {
        writeParquetFile();
        LOGGER.debug(String.format("Wrote (Overflow) => %,d\n", totalCounter.get()));
      }
    }
    stop();
    System.out.printf(" -> Exported %,d records\n", totalCounter.get());
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

  public static synchronized void writeParquetFile() {
    try {
      long fileNumber = fileCounter.incrementAndGet();
      String fileName = String.format("s3a://%s/%s/%s-%04d.parquet", s3Bucket, collection, collection, fileNumber);
      Path path = new Path(fileName);
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
          .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
          .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
          .withSchema(schema)
          .withConf(config)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withValidation(false)
          .withDictionaryEncoding(false)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .build()) {
        while (docQueue.peek() != null) {
          String document = docQueue.take();
          JsonNode node = mapper.readTree(document);
          writer.write(getRecord(schema, node));
          totalCounter.incrementAndGet();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void streamDocuments() {
    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        String content = DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8);
        try {
          docQueue.put(content);
          dataSizeCounter.addAndGet(content.length());
          flowController.ack(event);
          synchronized (WRITE_COORDINATOR) {
            if (!docQueue.isEmpty() && (dataSizeCounter.get() > 256_000_000)) {
              writeParquetFile();
              dataSizeCounter.set(0);
              LOGGER.debug(String.format("Wrote (Main) => %,d\n", totalCounter.get()));
            }
          }
        } catch (Exception e) {
          LOGGER.error("Error reading stream: {}", e.getMessage(), e);
        }
      }
      event.release();
    });
  }

  public static void createSystemEventHandler() {
    client.systemEventHandler(event -> {
      if (event instanceof StreamEndEvent) {
        StreamEndEvent streamEnd = (StreamEndEvent) event;
        if (streamEnd.partition() == 1023) {
          lastPartition.set(true);
        }
      }
    });
  }

  public void streamDocumentContents() {
    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        String content = DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8);
        try {
          docQueue.put(content);
          flowController.ack(event);
          docCount.incrementAndGet();
        } catch (Exception e) {
          LOGGER.error("Error reading stream: {}", e.getMessage(), e);
        }
      }
      event.release();
    });
  }

  public static void startToNow() {
    client.connect().block();
    client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).block();
    client.startStreaming().block();
  }

  public static void startFromNow() {
    client.connect().block();
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).block();
    client.startStreaming().block();
  }

  public <T> Stream<T> whileNotNull(Supplier<? extends T> supplier) {
    requireNonNull(supplier);
    return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.NONNULL) {
          @Override
          public boolean tryAdvance(Consumer<? super T> action) {
            do {
              T element = supplier.get();
              if (element != null) {
                action.accept(element);
                sentCount.incrementAndGet();
                return true;
              }
            } while (!client.sessionState().isAtEnd() || sentCount.get() < docCount.get());
            return false;
          }
        }, false);
  }

  public Stream<ByteBuffer> getDrain() {
    return whileNotNull(queue::poll);
  }

  public long getDocCount() {
    return docCount.get();
  }

  public Stream<String> getNextDocument() {
    return whileNotNull(docQueue::poll);
  }

  public BlockingQueue<String> getDocQueue() {
    return docQueue;
  }

  public boolean isDocQueueEmpty() {
    return docQueue.isEmpty();
  }

  public String getDocQueueEntry() {
    try {
      return docQueue.poll(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      return null;
    }
  }

  public boolean isAtEnd() {
    return client.sessionState().isAtEnd();
  }

  public Stream<ByteBuffer> getByCount(long count) {
    return Stream.generate(() -> {
          try {
            return queue.take();
          } catch (InterruptedException ex) {
            return null;
          }
        })
        .limit(count);
  }

  public static void stop() {
    client.disconnect().block();
  }

  public long getCount() {
    return docCount.get();
  }
}
