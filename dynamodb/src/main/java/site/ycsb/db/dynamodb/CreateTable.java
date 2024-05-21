package site.ycsb.db.dynamodb;

import org.apache.commons.cli.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Remove Table After Testing.
 */
public final class CreateTable {

  private enum PrimaryKeyType {
    HASH,
    HASH_AND_RANGE
  }

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "source properties");
    Option read = new Option("r", "rcu", true, "RCU value");
    Option write = new Option("w", "wcu", true, "WCU value");
    source.setRequired(true);
    read.setRequired(false);
    write.setRequired(false);
    options.addOption(source);
    options.addOption(read);
    options.addOption(write);

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

    long wcu = cmd.hasOption("wcu") ? Long.parseLong(cmd.getOptionValue("wcu")) : 0;
    long rcu = cmd.hasOption("rcu") ? Long.parseLong(cmd.getOptionValue("rcu")) : 0;

    try {
      properties.load(Files.newInputStream(Paths.get(propFile)));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }

    try {
      createTable(properties, rcu, wcu);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void createTable(Properties properties, long rcu, long wcu) {
    String endpoint = properties.getProperty("dynamodb.endpoint", null);
    String primaryKeyName = properties.getProperty("dynamodb.primaryKey", "userid");
    String hashKeyName = properties.getProperty("dynamodb.hashKeyName", "field0");
    String primaryKeyTypeString = properties.getProperty("dynamodb.primaryKeyType", "HASH");
    String configuredRegion = properties.getProperty("dynamodb.region", "us-east-1");
    String tableName = properties.getProperty("dynamodb.tableName", "usertable");
    long RCUParameter = Long.parseLong(properties.getProperty("dynamodb.RCU", "10000"));
    long WCUParameter = Long.parseLong(properties.getProperty("dynamodb.WCU", "10000"));
    String accessKeyID = properties.getProperty("dynamodb.accessKeyID",
        System.getenv("AWS_ACCESS_KEY_ID"));
    String secretAccessKey = properties.getProperty("dynamodb.secretAccessKey",
        System.getenv("AWS_SECRET_ACCESS_KEY"));
    String sessionToken = properties.getProperty("dynamodb.sessionToken",
        System.getenv("AWS_SESSION_TOKEN"));

    long RCU = rcu > 0 ? rcu : RCUParameter;
    long WCU = wcu > 0 ? wcu : WCUParameter;

    try {
      Region awsRegion = Region.of(configuredRegion);
      CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder();

      AwsCredentials credentials;
      if (sessionToken == null) {
        credentials = AwsBasicCredentials.create(accessKeyID, secretAccessKey);
      } else {
        credentials = AwsSessionCredentials.create(accessKeyID, secretAccessKey, sessionToken);
      }
      StaticCredentialsProvider provider = StaticCredentialsProvider.create(credentials);
      DynamoDbClientBuilder dynamoDBBuilder = DynamoDbClient.builder()
          .defaultsMode(DefaultsMode.AUTO)
          .region(awsRegion)
          .credentialsProvider(provider);
      if (endpoint != null) {
        dynamoDBBuilder.endpointOverride(URI.create(endpoint));
      }

      DynamoDbClient dynamoDB = dynamoDBBuilder.build();

      requestBuilder
          .attributeDefinitions(AttributeDefinition.builder()
              .attributeName(primaryKeyName)
              .attributeType(ScalarAttributeType.S)
              .build())
          .provisionedThroughput(ProvisionedThroughput.builder()
              .readCapacityUnits(RCU)
              .writeCapacityUnits(WCU)
              .build())
          .tableName(tableName);

      DynamoDbWaiter dbWaiter = dynamoDB.waiter();
      if (PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase()) == PrimaryKeyType.HASH) {
        requestBuilder
            .keySchema(KeySchemaElement.builder()
                .attributeName(primaryKeyName)
                .keyType(KeyType.HASH)
                .build());
      } else if (PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase()) == PrimaryKeyType.HASH_AND_RANGE) {
        requestBuilder
            .keySchema(KeySchemaElement.builder()
                    .attributeName(primaryKeyName)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(hashKeyName)
                    .keyType(KeyType.RANGE)
                    .build());
      }

      CreateTableRequest request = requestBuilder.build();
      dynamoDB.createTable(request);

      DescribeTableRequest tableRequest = DescribeTableRequest.builder()
          .tableName(tableName)
          .build();

      dbWaiter.waitUntilTableExists(tableRequest);
      System.err.printf("Created table %s with %d RCU / %d WCU\n", tableName, RCU, WCU);
    } catch (ResourceInUseException e) {
      System.err.printf("Table %s already exists\n", tableName);
    } catch (DynamoDbException e) {
      System.err.printf("Can not create table: %s\n", e.getMessage());
      e.printStackTrace(System.err);
    }
  }

  private CreateTable() {
    super();
  }

}
