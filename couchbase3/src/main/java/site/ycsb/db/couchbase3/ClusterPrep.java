package site.ycsb.db.couchbase3;

import org.apache.commons.cli.*;

public class ClusterPrep {
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";

  public static void clusterPrep(String hostname, String username, String password, String bucket,
                                 String project, String database) {
    try {
      if (project == null) {
        CouchbaseConnect db = new CouchbaseConnect(hostname, username, password);
      } else {
        CouchbaseConnect db = new CouchbaseConnect(hostname, username, password, project, database);
      }
    } catch (CouchbaseConnectException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    String hostname;
    String bucket;
    String password;
    String username;
    String project = null;
    String database = null;

    Option hostOpt = new Option("h", "hostname", true, "Host name");
    hostOpt.setRequired(true);
    options.addOption(hostOpt);

    Option userOpt = new Option("u", "user", true, "User name");
    userOpt.setRequired(false);
    options.addOption(userOpt);

    Option passOpt = new Option("p", "password", true, "Password");
    passOpt.setRequired(false);
    options.addOption(passOpt);

    Option bucketOpt = new Option("b", "bucket", true, "Bucket");
    bucketOpt.setRequired(true);
    options.addOption(bucketOpt);

    Option projectOpt = new Option("P", "project", true, "Project");
    projectOpt.setRequired(false);
    options.addOption(projectOpt);

    Option databaseOpt = new Option("D", "database", true, "Database");
    databaseOpt.setRequired(false);
    options.addOption(databaseOpt);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("TSCompare", options);
      System.exit(1);
    }

    hostname = cmd.getOptionValue("hostname");
    bucket = cmd.getOptionValue("bucket");
    if (cmd.hasOption("user")) {
      username = cmd.getOptionValue("user");
    } else {
      username = DEFAULT_USER;
    }
    if (cmd.hasOption("password")) {
      password = cmd.getOptionValue("password");
    } else {
      password = DEFAULT_PASSWORD;
    }
    if (cmd.hasOption("project")) {
      project = cmd.getOptionValue("project");
    }
    if (cmd.hasOption("database")) {
      database = cmd.getOptionValue("database");
    }

    try {
      clusterPrep(hostname, username, password, bucket, project, database);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      System.exit(1);
    }
  }
}
