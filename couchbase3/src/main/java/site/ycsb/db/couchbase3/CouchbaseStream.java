package site.ycsb.db.couchbase3;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.dcp.SecurityConfig;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

/**
 * Couchbase Stream Utility.
 */
public class CouchbaseStream {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.db.couchbase3.CouchbaseStream");
  private final String hostname;
  private final String username;
  private final String password;
  private final String bucket;
  private final String scope;
  private final String collection;
  private final Boolean useSsl;
  private boolean collectionEnabled;
  private final AtomicLong totalSize = new AtomicLong(0);
  private final AtomicLong docCount = new AtomicLong(0);
  private final AtomicLong sentCount = new AtomicLong(0);
  private final PriorityBlockingQueue<ByteBuffer> queue = new PriorityBlockingQueue<>();
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
        .build();

    client.controlEventHandler((flowController, event) -> {
      flowController.ack(event);
      event.release();
    });
  }

  public void streamDocuments() {
    client.dataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event)) {
        String key = MessageUtil.getCollectionIdAndKey(event, collectionEnabled).key();
        byte[] content = DcpMutationMessage.contentBytes(event);
        try {
          DocumentBuffer buffer = new DocumentBuffer(key, content);
          queue.add(buffer.getBuffer());
          docCount.incrementAndGet();
          totalSize.addAndGet(DcpMutationMessage.content(event).readableBytes());
        } catch (Exception e) {
          LOGGER.error("Error reading stream: {}", e.getMessage(), e);
        }
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

  public Stream<ByteBuffer> getDrain() {
    return whileNotNull(queue::poll);
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

  public void stop() {
    client.disconnect().block();
  }

  public long getCount() {
    return docCount.get();
  }
}
