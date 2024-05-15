package site.ycsb.db.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.apache.commons.cli.*;

import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Prepare Collection for Testing.
 */
public final class DropCollection {

  public static final String MONGO_URL_PROPERTY = "mongodb.url";
  public static final String MONGO_DB_PROPERTY = "mongodb.database";
  public static final String MONGO_COLLECTION_PROPERTY = "mongodb.collection";

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
      createCollection(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void createCollection(Properties properties) {
    String url = properties.getProperty(MONGO_URL_PROPERTY);
    String databaseName = properties.getProperty(MONGO_DB_PROPERTY);
    String collectionName = properties.getProperty(MONGO_COLLECTION_PROPERTY);

    ServerApi serverApi = ServerApi.builder()
        .version(ServerApiVersion.V1)
        .build();

    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(url))
        .serverApi(serverApi)
        .build();

    try (MongoClient mongoClient = MongoClients.create(settings)) {
      MongoDatabase database = mongoClient.getDatabase(databaseName);
      MongoCollection<Document> collection = database.getCollection(collectionName);
      collection.drop();
      System.err.printf("Dropped collection %s", collectionName);
    } catch (Exception e) {
      System.err.printf("mongo collection creation failed: %s\n", e.getMessage());
      e.printStackTrace(System.err);
    }
  }

  private DropCollection() {
    super();
  }

}
