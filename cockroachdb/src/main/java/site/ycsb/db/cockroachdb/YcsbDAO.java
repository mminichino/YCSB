package site.ycsb.db.cockroachdb;


import javax.sql.DataSource;
import java.sql.*;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class YcsbDAO {

  private static final int MAX_RETRY_COUNT = 3;
  private static final String RETRY_SQL_STATE = "40001";
  private static final boolean FORCE_RETRY = false;

  private final DataSource ds;

  private final Random rand = new Random();

  public YcsbDAO(DataSource ds) {
    this.ds = ds;
  }

  public PreparedStatement getInsertStatement(String table, String key, Set<String> fields) {
    String fieldString = String.join(",", fields);
    final String sql = "INSERT INTO " + table +
        " (" +
        key + "," +
        fieldString +
        ")" +
        " VALUES(?" +
        ",?".repeat(fields.size()) +
        ")";
    try (Connection connection = ds.getConnection()) {
      PreparedStatement preparedStatement = connection.prepareStatement(sql);
      return preparedStatement;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public Integer runSQL(PreparedStatement pstmt, String... args) {
    int rv = 0;

    try (Connection connection = ds.getConnection()) {
      connection.setAutoCommit(false);

      int retryCount = 0;

      while (retryCount <= MAX_RETRY_COUNT) {

        if (retryCount == MAX_RETRY_COUNT) {
          String err = String.format("hit max of %d retries, aborting", MAX_RETRY_COUNT);
          throw new RuntimeException(err);
        }

        try {
          for (int i=0; i<args.length; i++) {
            int place = i + 1;
            String arg = args[i];
            pstmt.setString(place, arg);
          }

          if (pstmt.execute()) {
            ResultSet rs = pstmt.getResultSet();
            ResultSetMetaData rsmeta = rs.getMetaData();
            int colCount = rsmeta.getColumnCount();

            while (rs.next()) {
              for (int i=1; i <= colCount; i++) {
                String name = rsmeta.getColumnName(i);
                String type = rsmeta.getColumnTypeName(i);
              }
            }
          } else {
            int updateCount = pstmt.getUpdateCount();
            rv += updateCount;
          }

          connection.commit();
          break;

        } catch (SQLException e) {

          if (RETRY_SQL_STATE.equals(e.getSQLState())) {
            System.out.printf("retryable exception occurred:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n", e.getSQLState(), e.getMessage(), retryCount);
            connection.rollback();
            retryCount++;
            int sleepMillis = (int)(Math.pow(2, retryCount) * 100) + rand.nextInt(100);
            System.out.printf("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis);
            try {
              Thread.sleep(sleepMillis);
            } catch (InterruptedException ignored) {}
            rv = -1;
          } else {
            throw e;
          }
        }
      }
    } catch (SQLException e) {
      System.out.printf("BasicExampleDAO.runSQL ERROR: { state => %s, cause => %s, message => %s }\n",
          e.getSQLState(), e.getCause(), e.getMessage());
      rv = -1;
    }

    return rv;
  }

}
