package site.ycsb.db.couchbase3;

import com.couchbase.client.core.deps.com.google.gson.JsonParser;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.dcp.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.core.deps.com.google.gson.JsonObject;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

public class CouchbaseStream {
  private String hostname;
  private String username;
  private String password;
  private String connectString;
  private String bucket;
  private String scope;
  private String collection;
  private Boolean useSsl;
  private String httpPrefix;
  private String couchbasePrefix;
  private String srvPrefix;
  private String adminPort;
  private String nodePort;
  private boolean collectionEnabled;
  private final AtomicLong totalSize = new AtomicLong(0);
  private final AtomicLong docCount = new AtomicLong(0);
  private final AtomicLong sentCount = new AtomicLong(0);
  private final PriorityBlockingQueue<String> queue = new PriorityBlockingQueue<>();
  private Client client;

  public CouchbaseStream(String hostname, String username, String password, String bucket, Boolean ssl) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.bucket = bucket;
    this.useSsl = ssl;
    this.scope = "_default";
    this.collection = "_default";
    this.init();
  }

  public CouchbaseStream(String hostname, String username, String password, String bucket, Boolean ssl,
                         String scope, String collection) {
    this.hostname = hostname;
    this.username = username;
    this.password = password;
    this.bucket = bucket;
    this.useSsl = ssl;
    this.scope = scope;
    this.collection = collection;
    this.init();
  }

  public void init() {
    StringBuilder connectBuilder = new StringBuilder();

    if (useSsl) {
      httpPrefix = "https://";
      couchbasePrefix = "couchbases://";
      srvPrefix = "_couchbases._tcp.";
      adminPort = "18091";
      nodePort = "19102";
    } else {
      httpPrefix = "http://";
      couchbasePrefix = "couchbase://";
      srvPrefix = "_couchbase._tcp.";
      adminPort = "8091";
      nodePort = "9102";
    }

    connectBuilder.append(couchbasePrefix);
    connectBuilder.append(hostname);

    connectString = connectBuilder.toString();

    collectionEnabled = !collection.equals("_default");

    Consumer<SecurityConfig.Builder> secClientConfig = securityConfig -> {
      securityConfig.enableTls(useSsl)
          .enableHostnameVerification(false)
          .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
    };

    client = Client.builder()
        .connectionString(connectString)
        .bucket(bucket)
        .securityConfig(secClientConfig)
        .credentials(username, password)
        .build();

    client.controlEventHandler((flowController, event) -> {
      flowController.ack(event);
      event.release();
    });
  }

  public void streamDocuments() {
    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        Date date = new Date();
        CollectionIdAndKey key = MessageUtil.getCollectionIdAndKey(event, collectionEnabled);
        String content = DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8);
        JsonObject metaObject = new JsonObject();
        metaObject.addProperty("id", key.key());
        metaObject.addProperty("timestamp", date.getTime());
        JsonObject docObject = JsonParser.parseString(content).getAsJsonObject();
        JsonObject rootObject = new JsonObject();
        rootObject.add("meta", metaObject);
        rootObject.add("document", docObject);
        queue.add(rootObject.toString());
        docCount.incrementAndGet();
        totalSize.addAndGet(DcpMutationMessage.content(event).readableBytes());
      }
      event.release();
    });
  }

  public void startToNow() {
    client.connect().block();
    client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).block();
    client.startStreaming().block();
  }

  public void startFromNow() {
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

  public Stream<String> getDrain() {
    return whileNotNull(queue::poll);
  }

  public Stream<String> getByCount(long count) {
    return Stream.generate(() -> {
          try {
            return queue.take();
          } catch (InterruptedException ex) {
            return null;
          }
        })
        .limit(count);
  }

  public void stop() {
    client.disconnect().block();
  }

  public long getCount() {
    return docCount.get();
  }
}
