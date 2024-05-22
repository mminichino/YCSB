package site.ycsb.db.couchbase3;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

import java.io.IOException;

public class ByteIteratorDeserializer extends JsonDeserializer<ByteIterator> {
  @Override
  public ByteIterator deserialize(JsonParser jp, DeserializationContext context) throws IOException {
    return new StringByteIterator(jp.readValueAs(String.class));
  }
}
