package site.ycsb.workloads;
import site.ycsb.*;
import site.ycsb.generator.ExponentialGenerator;
import site.ycsb.measurements.Measurements;

import java.util.*;

/**
 * A class that wraps the 3.x Couchbase SDK to be used with YCSB.
 */

public class SubDocWorkload extends CoreWorkload {

  private List<String> fieldnames;
  private boolean dataintegrity;
  private String arrayKey;

  private final Measurements measurements = Measurements.getMeasurements();

  @Override
  public void init(Properties p) throws WorkloadException {
    final String fieldnameprefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    fieldnames = new ArrayList<>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add(fieldnameprefix + i);
    }
    dataintegrity = Boolean.parseBoolean(
        p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    arrayKey = p.getProperty("subdoc.arrayKey", "DataArray");
    super.init(p);
  }

  private String buildDeterministicValue(String key, String fieldkey) {
    int size = fieldlengthgenerator.nextValue().intValue();
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  private HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  long nextKeynum() {
    long keynum;
    if (keychooser instanceof ExponentialGenerator) {
      do {
        keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
      } while (keynum < 0);
    } else {
      do {
        keynum = keychooser.nextValue().intValue();
      } while (keynum > transactioninsertkeysequence.lastValue());
    }
    return keynum;
  }

  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
    value.put(fieldkey, data);

    return value;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);
    HashMap<String, ByteIterator> values = buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      doTransactionScan(db);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

  public void doTransactionRead(DB db) {
    long keynum = nextKeynum();

    String keyname = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);
    HashSet<String> fields = null;
    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();

    db.read(table, keyname, fields, cells);
  }

  public void doTransactionReadModifyWrite(DB db) {
    long keynum = nextKeynum();

    String keyname = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);

    HashSet<String> fields = null;

    if (!readallfields) {
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      values = buildValues(keyname);
    } else {
      values = buildSingleValue(keyname);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();

    long ist = measurements.getIntendedStartTimeNs();
    long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    db.update(table, keyname, values);

    long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }

    measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
  }

  public void doTransactionScan(DB db) {
    long keynum = nextKeynum();

    String startkeyname = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);

    int len = scanlength.nextValue().intValue();

    HashSet<String> fields = null;

    if (!readallfields) {
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
  }

  public void doTransactionUpdate(DB db) {
    long keynum = nextKeynum();
    String keyname = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);
    HashMap<String, ByteIterator> values;

    values = buildValues(keyname);

    db.update(table, keyname, values);
  }

  public void doTransactionInsert(DB db) {
    long keynum = transactioninsertkeysequence.nextValue();

    try {
      String dbkey = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);

      HashMap<String, ByteIterator> values = buildValues(dbkey);
      db.insert(table, dbkey, values);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

}
