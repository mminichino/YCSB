package site.ycsb.db.couchbase3;

import org.apache.commons.cli.*;

/**
 * Compare Timestamps.
 */
public final class TSCompare {
  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd;

    Option source = new Option("s", "source", true, "source properties");
    source.setRequired(true);
    options.addOption(source);

    Option target = new Option("t", "target", true, "target properties");
    target.setRequired(true);
    options.addOption(target);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);

      System.exit(1);
    }
  }

  private TSCompare() {
    super();
  }
}
