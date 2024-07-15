package site.ycsb.db.couchbase3;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DocumentBuffer {
  ByteBuffer buffer;
  String key;
  byte[] content;

  public DocumentBuffer(String key, byte[] content) {
    byte[] contentSize = ByteBuffer.allocate(4).putInt(content.length).array();
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    byte[] keySize = ByteBuffer.allocate(4).putInt(keyBytes.length).array();
    this.buffer = ByteBuffer.allocate(contentSize.length + content.length + keySize.length + keyBytes.length);
    this.buffer.put(contentSize);
    this.buffer.put(content);
    this.buffer.put(keySize);
    this.buffer.put(keyBytes);
    this.buffer.flip();
    this.key = key;
    this.content = content;
  }

  public DocumentBuffer(ByteBuffer buffer) {
    int contentSize = buffer.getInt();
    byte[] content = new byte[contentSize];
    buffer.get(content);
    int keySize = buffer.getInt();
    byte[] keyBytes = new byte[keySize];
    buffer.get(keyBytes);
    this.buffer = buffer;
    this.key = new String(keyBytes, StandardCharsets.UTF_8);
    this.content = content;
  }

  public String getKey() {
    return this.key;
  }

  public byte[] getContent() {
    return this.content;
  }

  public ByteBuffer getBuffer() {
    return this.buffer;
  }
}
