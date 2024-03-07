package site.ycsb.db.couchbase3;

/**
 * Metric Functions.
 */
public enum MetricFunction {

  MAX("max"),
  AVG("avg");

  private final String value;

  MetricFunction(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
