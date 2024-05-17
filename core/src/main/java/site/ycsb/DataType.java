package site.ycsb;

/**
 * A class that represents a table column data type.
 */
public enum DataType {
  IDENTIFIER,
  INTEGER,
  LONG_INTEGER,
  FLOAT,
  LONG_FLOAT,
  DATE,
  FIXED_STRING,
  VARIABLE_STRING;

  public int length;

  DataType(int value) {
    this.length = value;
  }

  DataType() {
    this.length = 0;
  }

  public DataType setLength(int value) {
    this.length = value;
    return this;
  }
}
