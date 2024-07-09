package site.ycsb.db.snowflake.flavors;

import site.ycsb.db.snowflake.StatementType;

public abstract class DBFlavor {

  enum DBName {
    DEFAULT,
    BENCH
  }

  private final DBName dbName;

  public DBFlavor(DBName dbName) {
    this.dbName = dbName;
  }

  public static DBFlavor defaultDriver() {
    return new DefaultDBFlavor();
  }

  public static DBFlavor benchDriver() {
    return new BenchDBFlavor();
  }

  /**
   * Create and return a SQL statement for inserting data.
   */
  public abstract String createInsertStatement(StatementType insertType, String key);

  /**
   * Create and return a SQL statement for reading data.
   */
  public abstract String createReadStatement(StatementType readType, String key);

  /**
   * Create and return a SQL statement for deleting data.
   */
  public abstract String createDeleteStatement(StatementType deleteType, String key);

  /**
   * Create and return a SQL statement for updating data.
   */
  public abstract String createUpdateStatement(StatementType updateType, String key);

  /**
   * Create and return a SQL statement for scanning data.
   */
  public abstract String createScanStatement(StatementType scanType, String key,
                                             boolean sqlserverScans, boolean sqlansiScans);
}
