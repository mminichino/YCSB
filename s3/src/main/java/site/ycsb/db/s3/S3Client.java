/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * S3 storage client binding for YCSB.
 */
package site.ycsb.db.s3;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.io.InputStream;
import java.util.*;
import java.util.stream.IntStream;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * S3 Storage client for YCSB framework.
 * Properties to set:
 * s3.accessKeyId=access key S3 aws
 * s3.secretKey=secret key S3 aws
 * s3.endPoint=s3.amazonaws.com
 * s3.region=us-east-1
 * The parameter table is the name of the Bucket where to upload the files.
 * This must be created before to start the benchmark
 * The size of the file to upload is determined by two parameters:
 * - fieldcount this is the number of fields of a record in YCSB
 * - fieldlength this is the size in bytes of a single field in the record
 * together these two parameters define the size of the file to upload,
 * the size in bytes is given by the fieldlength multiplied by the fieldcount.
 * The name of the file is determined by the parameter key.
 *This key is automatically generated by YCSB.
 *
 */
public class S3Client extends DB {
  protected static final ch.qos.logback.classic.Logger LOGGER =
      (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("site.ycsb.db.s3.S3Client");
  private static AmazonS3 s3Client;

  /**
  * Initialize any state for the storage.
  * Called once per S3 instance; If the client is not null it is re-used.
  */
  @Override
  public void init() {
    synchronized (S3Client.class){
      Properties properties = getProperties();

      if (s3Client != null) {
        return;
      }

      String accessKeyId = properties.getProperty("s3.accessKeyId");
      LOGGER.info("Access key: " + accessKeyId);
      String secretKey = properties.getProperty("s3.secretKey");
      LOGGER.info("Secret key: " + secretKey);
      String endPoint = properties.getProperty("s3.endpoint", "s3.amazonaws.com");
      LOGGER.info("Endpoint: " + endPoint);
      String region = properties.getProperty("s3.region", "us-east-1");
      LOGGER.info("Region: " + region);
      int maxErrorRetry = Integer.parseInt(properties.getProperty("s3.maxErrorRetry", "15"));
      String protocol = properties.getProperty("s3.protocol", "HTTPS");
      LOGGER.info("Protocol: " + protocol);

      try {
        System.out.println("Initializing the S3 connection");

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setMaxErrorRetry(maxErrorRetry);
        if(protocol.equals("HTTP")) {
          clientConfig.setProtocol(Protocol.HTTP);
        } else {
          clientConfig.setProtocol(Protocol.HTTPS);
        }

        AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretKey);

        s3Client = AmazonS3ClientBuilder
            .standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, region))
            .withPathStyleAccessEnabled(true)
            .withClientConfiguration(clientConfig)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build();

        System.out.println("Connection successfully initialized");
      } catch (Exception e){
        logError(e);
      }
    }
  }

  private void logError(Exception error) {
    LOGGER.error(error.getMessage(), error);
  }

  /**
   * Cleanup any state for this storage.
   * Called once per S3 instance;
   */
  @Override
  public void cleanup() {
  }

  private static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      result.put(value.getKey(), value.getValue().toString());
    }
    return result;
  }

  /**
   * Delete a file from S3 Storage.
   *
   * @param bucket
   *            The name of the bucket
   * @param key
   * The record key of the file to delete.
   * @return OK on success, otherwise ERROR. See the
   * {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String bucket, String key) {
    try {
      deleteFromStorage(bucket, key);
      return Status.OK;
    } catch (Throwable t){
      LOGGER.error("delete transaction exception: " + t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
  * Create a new File in the Bucket. Any field/value pairs in the specified
  * values HashMap will be written into the file with the specified record
  * key.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *      The record key of the file to insert.
  * @param values
  *            A HashMap of field/value pairs to insert in the file.
  *            Only the content of the first field is written to a byteArray
  *            multiplied by the number of field. In this way the size
  *            of the file to upload is determined by the fieldlength
  *            and fieldcount parameters.
  * @return OK on success, ERROR otherwise. See the
  *         {@link DB} class's description for a discussion of error codes.
  */
  @Override
  public Status insert(String bucket, String key, Map<String, ByteIterator> values) {
    try {
      writeToStorage(bucket, key, values);
      return Status.OK;
    } catch (Throwable t){
      LOGGER.error("insert transaction exception: " + t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
  * Read a file from the Bucket. Each field/value pair from the result
  * will be stored in a HashMap.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The record key of the file to read.
  * @param fields
  *            The list of fields to read, or null for all of them,
  *            it is null by default
  * @param result
  *          A HashMap of field/value pairs for the result
  * @return OK on success, ERROR otherwise.
  */
  @Override
  public Status read(String bucket, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      readFromStorage(bucket, key);
      return Status.OK;
    } catch (Throwable t){
      LOGGER.error("read transaction exception: " + t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
  * Update a file in the database. Any field/value pairs in the specified
  * values HashMap will be written into the file with the specified file
  * key, overwriting any existing values with the same field name.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The file key of the file to write.
  * @param values
  *            A HashMap of field/value pairs to update in the record
  * @return OK on success, ERROR otherwise.
  */
  @Override
  public Status update(String bucket, String key, Map<String, ByteIterator> values) {
    try {
      writeToStorage(bucket, key, values);
      return Status.OK;
    } catch (Throwable t){
      LOGGER.error("update transaction exception: " + t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
  * Perform a range scan for a set of files in the bucket. Each
  * field/value pair from the result will be stored in a HashMap.
  *
  * @param bucket
  *            The name of the bucket
  * @param startkey
  *            The file key of the first file to read.
  * @param recordcount
  *            The number of files to read
  * @param fields
  *            The list of fields to read, or null for all of them
  * @param result
  *            A Vector of HashMaps, where each HashMap is a set field/value
  *            pairs for one file
  * @return OK on success, ERROR otherwise.
  */
  @Override
  public Status scan(String bucket, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      scanFromStorage(bucket, startkey, recordcount);
      return Status.OK;
    } catch (Throwable t){
      LOGGER.error("scan transaction exception: " + t.getMessage(), t);
      return Status.ERROR;
    }
  }

  /**
  * Upload a new object to S3 or update an object on S3.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The file key of the object to upload/update.
  * @param values
  *            The data to be written on the object
  */
  protected void writeToStorage(String bucket, String key, Map<String, ByteIterator> values) {
    String jsonData;
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      jsonData = objectMapper.writeValueAsString(encode(values));
      s3Client.putObject(bucket, key, jsonData);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
  * Download an object from S3.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The file key of the object to upload/update.
  */
  protected void readFromStorage(String bucket, String key) {
    try {
      S3Object object = s3Client.getObject(bucket, key);
      InputStream inputStream = object.getObjectContent();
      inputStream.close();
      object.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
  * Perform an emulation of a database scan operation on a S3 bucket.
  *
  * @param bucket
  *            The name of the bucket
  * @param startkey
  *            The file key of the first file to read.
  * @param recordcount
  *            The number of files to read
  */
  protected void scanFromStorage(String bucket, String startkey, int recordcount) {
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

    Collections.sort(keyList);

    OptionalInt indexOpt = IntStream.range(0, keyList.size())
        .filter(i -> startkey.equals(keyList.get(i)))
        .findFirst();

    int startIndex = indexOpt.isPresent() ? indexOpt.getAsInt() : 0;
    int endIndex = Math.min(startIndex + recordcount, keyList.size());

    for (int i = startIndex; i < endIndex; i++){
      readFromStorage(bucket, keyList.get(i));
    }
  }

  protected void deleteFromStorage(String bucket, String key) {
    s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
  }
}