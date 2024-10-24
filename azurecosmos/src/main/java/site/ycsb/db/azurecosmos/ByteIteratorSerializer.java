package site.ycsb.db.azurecosmos;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import site.ycsb.ByteIterator;

import java.io.IOException;

public class ByteIteratorSerializer extends JsonSerializer<ByteIterator> {
  @Override
  public void serialize(ByteIterator value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
    if (value.valueType == ByteIterator.valueDataType.LONG) {
      gen.writeNumber(value.toLong());
    } else {
      gen.writeString(value.toString());
    }
  }
}
