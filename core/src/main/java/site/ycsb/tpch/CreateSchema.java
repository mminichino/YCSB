package site.ycsb.tpch;

import site.ycsb.DataType;

import java.util.*;

/**
 * A class that represents the TPC-H schema.
 */
public abstract class CreateSchema {
  Map<String, DataType> partTable = new HashMap<>() {{
    put("p_partkey", DataType.IDENTIFIER);
    put("p_name", DataType.VARIABLE_STRING.setLength(55));
    put("p_mfgr", DataType.FIXED_STRING.setLength(25));
    put("p_brand", DataType.FIXED_STRING.setLength(10));
    put("p_type", DataType.VARIABLE_STRING.setLength(25));
    put("p_size", DataType.INTEGER);
    put("p_container", DataType.FIXED_STRING.setLength(10));
    put("p_retailprice", DataType.FLOAT);
    put("p_comment", DataType.VARIABLE_STRING.setLength(23));
  }};
  Set<String> partKeys = new HashSet<>(List.of("p_partkey"));
  Integer partScaleFactor = 200000;

  Map<String, DataType> supplierTable = new HashMap<>() {{
    put("s_suppkey", DataType.IDENTIFIER);
    put("s_name", DataType.FIXED_STRING.setLength(25));
    put("s_address", DataType.VARIABLE_STRING.setLength(40));
    put("s_nationkey", DataType.IDENTIFIER);
    put("s_phone", DataType.FIXED_STRING.setLength(15));
    put("s_acctbal", DataType.FLOAT);
    put("s_comment", DataType.VARIABLE_STRING.setLength(101));
  }};
  Set<String> supplierKeys = new HashSet<>(List.of("s_suppkey"));
  Integer supplierScaleFactor = 10000;

  Map<String, DataType> partSuppTable = new HashMap<>() {{
    put("ps_partkey", DataType.IDENTIFIER);
    put("ps_suppkey", DataType.IDENTIFIER);
    put("ps_availqty", DataType.INTEGER);
    put("ps_supplycost", DataType.FLOAT);
    put("ps_comment", DataType.VARIABLE_STRING.setLength(199));
  }};
  Set<String> partSuppKeys = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));

  Map<String, DataType> customerTable = new HashMap<>() {{
    put("c_custkey", DataType.IDENTIFIER);
    put("c_name", DataType.VARIABLE_STRING.setLength(25));
    put("c_address", DataType.VARIABLE_STRING.setLength(40));
    put("c_nationkey", DataType.IDENTIFIER);
    put("c_phone", DataType.FIXED_STRING.setLength(15));
    put("c_acctbal", DataType.FLOAT);
    put("c_mktsegment", DataType.FIXED_STRING.setLength(10));
    put("c_comment", DataType.VARIABLE_STRING.setLength(117));
  }};
  Set<String> customerKeys = new HashSet<>(List.of("c_custkey"));
  Integer customerScaleFactor = 150000;

  Map<String, DataType> ordersTable = new HashMap<>() {{
    put("o_orderkey", DataType.IDENTIFIER);
    put("o_custkey", DataType.IDENTIFIER);
    put("o_orderstatus", DataType.FIXED_STRING.setLength(1));
    put("o_totalprice", DataType.FLOAT);
    put("o_orderdate", DataType.DATE);
    put("o_orderpriority", DataType.FIXED_STRING.setLength(15));
    put("o_clerk", DataType.FIXED_STRING.setLength(15));
    put("o_shippriority", DataType.INTEGER);
    put("o_comment", DataType.VARIABLE_STRING.setLength(79));
  }};
  Set<String> ordersKeys = new HashSet<>(List.of("o_orderkey"));
  Integer ordersScaleFactor = 1500000;

  Map<String, DataType> lineItemTable = new HashMap<>() {{
    put("l_orderkey", DataType.IDENTIFIER);
    put("l_partkey", DataType.IDENTIFIER);
    put("l_suppkey", DataType.IDENTIFIER);
    put("l_linenumber", DataType.INTEGER);
    put("l_quantity", DataType.FLOAT);
    put("l_extendedprice", DataType.FLOAT);
    put("l_discount", DataType.FLOAT);
    put("l_tax", DataType.FLOAT);
    put("l_returnflag", DataType.FIXED_STRING.setLength(1));
    put("l_linestatus", DataType.FIXED_STRING.setLength(1));
    put("l_shipdate", DataType.DATE);
    put("l_commitdate", DataType.DATE);
    put("l_receiptdate", DataType.DATE);
    put("l_shipinstruct", DataType.FIXED_STRING.setLength(25));
    put("l_shipmode", DataType.FIXED_STRING.setLength(10));
    put("l_comment", DataType.VARIABLE_STRING.setLength(44));
  }};
  Set<String> lineItemKeys = new HashSet<>(List.of("l_orderkey", "l_linenumber"));

  Map<String, DataType> nationTable = new HashMap<>() {{
    put("n_nationkey", DataType.IDENTIFIER);
    put("n_name", DataType.FIXED_STRING.setLength(25));
    put("n_regionkey", DataType.IDENTIFIER);
    put("n_comment", DataType.VARIABLE_STRING.setLength(152));
  }};
  Set<String> nationKeys = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));

  Map<String, DataType> regionTable = new HashMap<>() {{
    put("r_regionkey", DataType.IDENTIFIER);
    put("r_name", DataType.FIXED_STRING.setLength(25));
    put("r_comment", DataType.VARIABLE_STRING.setLength(152));
  }};
  Set<String> regionKeys = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));

  Set<String> type1Set = new HashSet<>(List.of("STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"));
  Set<String> type2Set = new HashSet<>(List.of("ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"));
  Set<String> type3Set = new HashSet<>(List.of("TIN", "NICKEL", "BRASS", "STEEL", "COPPER"));

  Set<String> container1Set = new HashSet<>(List.of("SM", "LG", "MED", "JUMBO", "WRAP"));
  Set<String> container2Set = new HashSet<>(List.of("CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"));

  List<String> segments = List.of("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD");

  List<String> priorities = List.of("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW");

  List<String> instructions = List.of("DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN");

  List<String> modes = List.of("REG AIR", "AIR RAIL", "SHIP", "TRUCK", "MAIL", "FOB");

  List<String> nouns = List.of(
      "foxes", "ideas", "theodolites", "pinto beans",
      "instructions", "dependencies", "excuses", "platelets",
      "asymptotes", "courts", "dolphins", "multipliers",
      "sauternes", "warthogs", "frets", "dinos",
      "attainments", "somas", "Tiresias'", "patterns",
      "forges", "braids", "hockey players", "frays",
      "warhorses", "dugouts", "notornis", "epitaphs",
      "pearls", "tithes", "waters", "orbits",
      "gifts", "sheaves", "depths", "sentiments",
      "decoys", "realms", "pains", "grouches",
      "escapades"
  );

  List<String> verbs = List.of(
      "sleep", "wake", "are", "cajole",
      "haggle", "nag", "use", "boost",
      "affix", "detect", "integrate", "maintain",
      "nod", "was", "lose", "sublate",
      "solve", "thrash", "promise", "engage",
      "hinder", "print",  "x-ray", "breach",
      "eat", "grow", "impress", "mold",
      "poach", "serve", "run", "dazzle",
      "snooze", "doze", "unwind", "kindle",
      "play", "hang", "believe", "doubt"
  );

  List<String> adjectives = List.of(
      "furious", "sly", "careful", "blithe",
      "quick", "fluffy", "slow", "quiet",
      "ruthless", "thin", "close", "dogged",
      "daring", "brave", "stealthy", "permanent",
      "enticing", "idle", "busy", "regular",
      "final", "ironic", "even", "bold",
      "silent"
  );

  List<String> adverbs = List.of(
      "sometimes", "always", "never", "furiously",
      "slyly", "carefully", "blithely", "quickly",
      "fluffily", "slowly", "quietly", "ruthlessly",
      "thinly", "closely", "doggedly", "daringly",
      "bravely", "stealthily", "permanently", "enticingly",
      "idly", "busily", "regularly", "finally",
      "ironically", "evenly", "boldly", "silently"
  );

  List<String> prepositions = List.of(
      "about", "above", "according to", "across",
      "after", "against", "along", "alongside of",
      "among", "around", "at", "atop",
      "before", "behind", "beneath", "beside",
      "besides", "between", "beyond", "by",
      "despite", "during", "except", "for",
      "from", "in place of", "inside", "instead of",
      "into", "near", "of", "on",
      "outside", "over", "past", "since",
      "through", "throughout", "to", "toward",
      "under", "until", "up", "upon",
      "without", "with", "within"
  );

  List<String> auxiliaries = List.of(
      "do", "may", "might", "shall",
      "will", "would", "can", "could",
      "should", "ought to", "must", "will have to",
      "shall have to", "could have to", "should have to", "must have to",
      "need to", "try to"
  );

  List<String> terminators = List.of(".", ";", ":", "?", "!", "--");
}
