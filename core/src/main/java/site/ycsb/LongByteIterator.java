package site.ycsb;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LongByteIterator extends ByteIterator {
  private final Long value;
  private int off;

  public static void putAllAsByteIterators(Map<String, ByteIterator> out, Map<String, Long> in) {
    for (Map.Entry<String, Long> entry : in.entrySet()) {
      out.put(entry.getKey(), new LongByteIterator(entry.getValue()));
    }
  }

  public static void putAllAsStrings(Map<String, Long> out, Map<String, ByteIterator> in) {
    for (Map.Entry<String, ByteIterator> entry : in.entrySet()) {
      out.put(entry.getKey(), entry.getValue().toLong());
    }
  }

  public static Map<String, ByteIterator> getByteIteratorMap(Map<String, Long> m) {
    HashMap<String, ByteIterator> ret = new HashMap<>();

    for (Map.Entry<String, Long> entry : m.entrySet()) {
      ret.put(entry.getKey(), new LongByteIterator(entry.getValue()));
    }
    return ret;
  }

  public static Map<String, Long> getLongMap(Map<String, ByteIterator> m) {
    HashMap<String, Long> ret = new HashMap<>();

    for (Map.Entry<String, ByteIterator> entry : m.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().toLong());
    }
    return ret;
  }

  public LongByteIterator(Long n) {
    valueType = valueDataType.LONG;
    this.value = n;
    this.off = 0;
  }

  @Override
  public boolean hasNext() {
    return off < Long.BYTES;
  }

  @Override
  public byte nextByte() {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(value);
    byte b = buffer.array()[off];
    off++;
    return b;
  }

  @Override
  public long bytesLeft() {
    return Long.BYTES - off;
  }

  @Override
  public void reset() {
    off = 0;
  }

  @Override
  public byte[] toArray() {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(value);
    return buffer.array();
  }

  @Override
  public Long toLong() {
    if (off > 0) {
      return super.toLong();
    } else {
      return value;
    }
  }
}
