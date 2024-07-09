package site.ycsb;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TableKeys {
  public String primaryKeyName;
  public TableKeyType primaryKeyType;
  public List<String> foreignKeyNames = new ArrayList<>();
  public List<TableKeyType> foreignKeyTypes = new ArrayList<>();

  public TableKeys create(String name, TableKeyType type) {
    this.primaryKeyName = name;
    this.primaryKeyType = type;
    return this;
  }

  public TableKeys addForeignKey(String name, TableKeyType type) {
    foreignKeyNames.add(name);
    foreignKeyTypes.add(type);
    return this;
  }

  public String getDocumentId(ObjectNode data) {
    List<String> foreignKeys = foreignKeyNames.stream().map(key -> data.get(key).asText()).collect(Collectors.toList());
    return data.get(primaryKeyName).asText() + "." + String.join(".", foreignKeys);
  }

  public List<String> createKeyList() {
    List<String> indexFields = new ArrayList<>();
    indexFields.add(primaryKeyName);
    indexFields.addAll(foreignKeyNames);
    return indexFields;
  }
}
