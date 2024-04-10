package site.ycsb.db.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A class that cleans up a S3 bucket.
 */
public final class BucketCleanup {
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
      formatter.printHelp("BucketCleanup", options);
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
      String accessKeyId = properties.getProperty("s3.accessKeyId");
      System.out.println("Access key: " + accessKeyId);
      String secretKey = properties.getProperty("s3.secretKey");
      System.out.println("Secret key: " + secretKey);
      String endPoint = properties.getProperty("s3.endpoint", "s3.amazonaws.com");
      System.out.println("Endpoint: " + endPoint);
      String region = properties.getProperty("s3.region", "us-east-1");
      System.out.println("Region: " + region);
      int maxErrorRetry = Integer.parseInt(properties.getProperty("s3.maxErrorRetry", "15"));
      String protocol = properties.getProperty("s3.protocol", "HTTPS");
      System.out.println("Protocol: " + protocol);
      String bucket = properties.getProperty("table", "ycsb");
      System.out.println("Bucket: " + bucket);

      ClientConfiguration clientConfig = new ClientConfiguration();
      clientConfig.setMaxErrorRetry(maxErrorRetry);
      if(protocol.equals("HTTP")) {
        clientConfig.setProtocol(Protocol.HTTP);
      } else {
        clientConfig.setProtocol(Protocol.HTTPS);
      }

      AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretKey);

      AmazonS3 s3Client = AmazonS3ClientBuilder
          .standard()
          .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, region))
          .withPathStyleAccessEnabled(true)
          .withClientConfiguration(clientConfig)
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .build();

      ObjectListing listing = s3Client.listObjects(bucket);
      List<S3ObjectSummary> summaries = listing.getObjectSummaries();
      List<String> keyList = new ArrayList<>();

      while (listing.isTruncated()) {
        listing = s3Client.listNextBatchOfObjects(listing);
        summaries.addAll(listing.getObjectSummaries());
      }

      for (S3ObjectSummary summary : summaries) {
        String summaryKey = summary.getKey();
        keyList.add(summaryKey);
      }

      System.out.printf("Total Objects: %d\n", keyList.size());

      int i = 1;
      for (String key : keyList) {
        s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
        System.out.printf("Deleting document: %d\r", i++);
      }

      System.out.print("Done.\n");
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private BucketCleanup() {
    super();
  }
}
