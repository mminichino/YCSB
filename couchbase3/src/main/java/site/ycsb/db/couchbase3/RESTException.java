package site.ycsb.db.couchbase3;

/**
 * RESTInterface Exception Class.
 */
public class RESTException extends Exception {
  private Integer code = 0;

  public RESTException(Integer code, String message) {
    super(message);
    this.code = code;
  }

  public RESTException(String message) {
    super(message);
  }

  public Integer getCode() {
    return code;
  }
}
