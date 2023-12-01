package site.ycsb.db.couchbase3;

/**
 * Lookup HTTP Error Codes.
 */
public enum ErrorCode {

  BADREQUEST(400),
  NOTAUTHORIZED(401),
  FORBIDDEN(403),
  NOTIMPLEMENTED(404),
  CONFLICTEXCEPTION(409),
  PRECONDITIONFAILED(412),
  INVALIDBODYCONTENTS(415),
  REQUESTVALIDATIONERROR(422),
  INTERNALSERVERERROR(500),
  SYNCGATEWAYOPERATIONEXCEPTION(503);

  private final int value;

  ErrorCode(int value) {
    this.value = value;
  }

  public static ErrorCode valueOf(int value) {
    for (ErrorCode c : ErrorCode.values()) {
      if (c.value == value) {
        return c;
      }
    }
    throw new IllegalArgumentException(String.format("Error code %d not found", value));
  }
}
