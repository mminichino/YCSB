package site.ycsb.db.cockroachdb;

import org.apache.commons.cli.*;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.jooq.*;
import org.jooq.impl.*;

public class CockroachDBSetup {
  public static final String CONNECTION_URL = "db.url";
  public static final String CONNECTION_USER = "db.user";
  public static final String CONNECTION_PASSWD = "db.passwd";
  public static final String CONNECTION_SSL = "db.ssl";

  public static final String TABLE_NAME_PROPERTY = "db.tablename";
  public static final String TABLE_NAME_PROPERTY_DEFAULT = "usertable";

  public static final String FIELD_COUNT_PROPERTY = "fieldcount";
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  public static final String YCSB_KEY = "id";

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "source properties");
    source.setRequired(true);
    options.addOption(source);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("CreateDatabase", options);
      System.exit(1);
    }

    String propFile = cmd.getOptionValue("properties");

    try {
      properties.load(Files.newInputStream(Paths.get(propFile)));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }

    try {
      createTable(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void createTable(Properties properties) {
    String url = properties.getProperty(CONNECTION_URL);
    String user = properties.getProperty(CONNECTION_USER);
    String passwd = properties.getProperty(CONNECTION_PASSWD);
    boolean ssl = Boolean.parseBoolean(properties.getProperty(CONNECTION_SSL, "true"));

    String table = properties.getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);
    int fieldcount = Integer.parseInt(properties.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    try {
      PGSimpleDataSource ds = new PGSimpleDataSource();
      ds.setUrl(url);
      ds.setUser(user);
      ds.setPassword(passwd);
      ds.setSsl(ssl);
      Connection connection = ds.getConnection();
      connection.setAutoCommit(false);

      DSLContext create = DSL.using(connection, SQLDialect.POSTGRES);

      try (DropTableStep drop = create.dropTableIfExists(table)) {
        PreparedStatement dropStmt = connection.prepareStatement(drop.getSQL());
        dropStmt.execute();
        connection.commit();
      }

      try (CreateTableColumnStep createTable = create.createTable(table).column(YCSB_KEY, SQLDataType.VARCHAR(255).nullable(false))) {
        for (int idx = 0; idx < fieldcount; idx++) {
          createTable.column(String.format("field%d", idx), SQLDataType.VARCHAR(100).nullable(false));
        }
        createTable.primaryKey(YCSB_KEY);
        PreparedStatement createStmt = connection.prepareStatement(createTable.getSQL());
        createStmt.execute();
        connection.commit();
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private CockroachDBSetup() {
    super();
  }
}
