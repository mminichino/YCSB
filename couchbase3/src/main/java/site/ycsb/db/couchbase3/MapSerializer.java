package site.ycsb.db.couchbase3;

import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import site.ycsb.ByteIterator;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MapSerializer implements JsonSerializer {

  @Override
  public byte[] serialize(Object input) {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ByteIteratorSerializer");
    module.addSerializer(ByteIterator.class, new ByteIteratorSerializer());
    mapper.registerModule(module);
    try {
      return mapper.writeValueAsBytes(input);
    } catch (JsonProcessingException e) {
      throw new EncodingFailureException("Serializing of content + " + redactUser(input) + " to JSON failed.", e);
    }
  }

  @Override
  public <T> T deserialize(Class<T> target, byte[] input) {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ByteIteratorDeserializer");
    module.addDeserializer(ByteIterator.class, new ByteIteratorDeserializer());
    mapper.registerModule(module);
    try {
      return mapper.readValue(input, target);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
          + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }

  @Override
  public <T> T deserialize(TypeRef<T> target, byte[] input) {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("ByteIteratorDeserializer");
    module.addDeserializer(ByteIterator.class, new ByteIteratorDeserializer());
    mapper.registerModule(module);
    JavaType type = mapper.getTypeFactory().constructType(target.type());
    try {
      return mapper.readValue(input, type);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
          + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }
}
