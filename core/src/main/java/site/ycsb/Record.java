package site.ycsb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Set;
import java.util.stream.Collectors;

public class Record {
  private final ObjectNode data;
  private Set<String> primaryKey;
  private String table;

  public Record(String table) {
    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.table = table;
  }

  public String tableName() {
    return table;
  }

  public void setKey(Set<String> key) {
    primaryKey = key;
  }

  public void add(String field, int value) {
    this.data.put(field, value);
  }

  public void add(String field, Integer value) {
    this.data.put(field, value);
  }

  public void add(String field, float value) {
    this.data.put(field, value);
  }

  public void add(String field, Float value) {
    this.data.put(field, value);
  }

  public void add(String field, double value) {
    this.data.put(field, value);
  }

  public void add(String field, Double value) {
    this.data.put(field, value);
  }

  public void add(String field, String value) {
    this.data.put(field, value);
  }

  public void add(String field) {
    this.data.putNull(field);
  }

  public String getString(String key) {
    return this.data.get(key).asText();
  }

  public Set<String> getKeyValues() {
    return primaryKey.stream().map(this::getString).collect(Collectors.toSet());
  }

  public ObjectNode contents() {
    return this.data;
  }

  public Set<String> getKey() {
    return this.primaryKey;
  }
}
