package site.ycsb.db.snowflake.flavors;

import site.ycsb.db.snowflake.SnowflakeClient;
import site.ycsb.db.snowflake.StatementType;

/**
 * A default flavor for relational databases.
 */
public class DefaultDBFlavor extends DBFlavor {
  public DefaultDBFlavor() {
    super(DBName.DEFAULT);
  }
  public DefaultDBFlavor(DBName dbName) {
    super(dbName);
  }

  @Override
  public String createInsertStatement(StatementType insertType, String key) {
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + SnowflakeClient.PRIMARY_KEY + "," + insertType.getFieldString() + ")");
    insert.append(" VALUES(?");
    for (int i = 0; i < insertType.getNumFields(); i++) {
      insert.append(",?");
    }
    insert.append(")");
    return insert.toString();
  }

  @Override
  public String createReadStatement(StatementType readType, String key) {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(readType.getTableName());
    read.append(" WHERE ");
    read.append(SnowflakeClient.PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  @Override
  public String createDeleteStatement(StatementType deleteType, String key) {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(SnowflakeClient.PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }

  @Override
  public String createUpdateStatement(StatementType updateType, String key) {
    String[] fieldKeys = updateType.getFieldString().split(",");
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    for (int i = 0; i < fieldKeys.length; i++) {
      update.append(fieldKeys[i]);
      update.append("=?");
      if (i < fieldKeys.length - 1) {
        update.append(", ");
      }
    }
    update.append(" WHERE ");
    update.append(SnowflakeClient.PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  @Override
  public String createScanStatement(StatementType scanType, String key, boolean sqlserverScans, boolean sqlansiScans) {
    StringBuilder select;
    if (sqlserverScans) {
      select = new StringBuilder("SELECT TOP (?) * FROM ");
    } else {
      select = new StringBuilder("SELECT * FROM ");
    }
    select.append(scanType.getTableName());
    select.append(" WHERE ");
    select.append(SnowflakeClient.PRIMARY_KEY);
    select.append(" >= ?");
    select.append(" ORDER BY ");
    select.append(SnowflakeClient.PRIMARY_KEY);
    if (!sqlserverScans) {
      if (sqlansiScans) {
        select.append(" FETCH FIRST ? ROWS ONLY");
      } else {
        select.append(" LIMIT ?");
      }
    }
    return select.toString();
  }
}
