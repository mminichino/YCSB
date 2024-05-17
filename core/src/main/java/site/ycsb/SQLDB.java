package site.ycsb;

import com.google.gson.JsonObject;

import java.util.Properties;
import java.util.Set;
import java.util.Map;

/**
 * SQL Database Driver.
 */
public abstract class SQLDB {
  private Properties properties = new Properties();

  public void setProperties(Properties p) {
    properties = p;
  }

  public Properties getProperties() {
    return properties;
  }

  public void init() throws DBException {
  }

  public void cleanup() throws DBException {
  }

  public abstract boolean createTable(String tableName,
                                      Set<String> pkFields,
                                      Map<String, DataType> tableFields);

  public abstract boolean addRow(String tableName, JsonObject row);

  public abstract JsonObject query(String sql);

}
