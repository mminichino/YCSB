package site.ycsb.db.dynamodb;

import org.apache.commons.cli.*;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Remove Table After Testing.
 */
public final class DropTable {

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
      dropTable(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void dropTable(Properties properties) {
    String endpoint = properties.getProperty("dynamodb.endpoint", null);
    String configuredRegion = properties.getProperty("dynamodb.region", "us-east-1");
    String credentialProfile = properties.getProperty("dynamodb.profile", "default");
    String tableName = properties.getProperty("dynamodb.tableName", "usertable");

    Region awsRegion = Region.of(configuredRegion);

    ProfileCredentialsProvider provider = ProfileCredentialsProvider.create(credentialProfile);
    DynamoDbClientBuilder dynamoDBBuilder = DynamoDbClient.builder()
        .region(awsRegion)
        .credentialsProvider(provider);
    if (endpoint != null) {
      dynamoDBBuilder.endpointOverride(URI.create(endpoint));
    }

    DynamoDbClient dynamoDB = dynamoDBBuilder.build();
    DynamoDbWaiter dbWaiter = dynamoDB.waiter();

    DeleteTableRequest request = DeleteTableRequest.builder()
        .tableName(tableName)
        .build();

    try {
      dynamoDB.deleteTable(request);
      DescribeTableRequest tableRequest = DescribeTableRequest.builder()
          .tableName(tableName)
          .build();
      dbWaiter.waitUntilTableNotExists(tableRequest);
      System.err.printf("Dropped table %s\n", tableName);
    } catch (ResourceNotFoundException e) {
      System.err.printf("Table %s does not exist\n", tableName);
    } catch (DynamoDbException e) {
      System.err.printf("Can not delete table: %s\n", e.getMessage());
      e.printStackTrace(System.err);
    }
  }

  private DropTable() {
    super();
  }

}
