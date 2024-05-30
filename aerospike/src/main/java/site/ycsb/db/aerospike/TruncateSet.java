package site.ycsb.db.aerospike;

import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class TruncateSet {

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";
  private static final String DEFAULT_SET = "usertable";

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
      truncateSet(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void truncateSet(Properties props) {
    String namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);
    String set = props.getProperty("as.set", DEFAULT_SET);
    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    int timeout = Integer.parseInt(props.getProperty("as.timeout", DEFAULT_TIMEOUT));
    boolean external = props.getProperty("as.external", "false").equals("true");

    ClientPolicy clientPolicy = new ClientPolicy();
    InfoPolicy infoPolicy = new InfoPolicy();

    if (user != null && password != null) {
      clientPolicy.user = user;
      clientPolicy.password = password;
      clientPolicy.timeout = timeout;
    }

    if (external) {
      clientPolicy.useServicesAlternate = true;
    }

    try (com.aerospike.client.AerospikeClient client =
             new com.aerospike.client.AerospikeClient(clientPolicy, host, port)) {
      client.truncate(infoPolicy, namespace, set, null);
      System.err.printf("Truncated set %s\n", set);

      Policy policy = new Policy();
      policy.socketTimeout = 0;
      IndexTask task = client.createIndex(policy, namespace, set, "ididx", "id", IndexType.NUMERIC);
      task.waitTillComplete();
      System.err.println("Created index on id");
    } catch (AerospikeException ae) {
      if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
        throw ae;
      }
    } catch (Exception e) {
      System.err.printf("set truncate failed failed: %s\n", e.getMessage());
      e.printStackTrace(System.err);
    }
  }

  private TruncateSet() {
    super();
  }

}
