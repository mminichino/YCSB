package site.ycsb.tpc;

import site.ycsb.DataType;

import java.util.*;

public class Tables {
  public static Map<String, DataType> partTableH = new HashMap<>() {{
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
  public static Set<String> partKeysH = new HashSet<>(List.of("p_partkey"));
  public static Integer partScaleFactorH = 200000;

  public static Map<String, DataType> supplierTableH = new HashMap<>() {{
    put("s_suppkey", DataType.IDENTIFIER);
    put("s_name", DataType.FIXED_STRING.setLength(25));
    put("s_address", DataType.VARIABLE_STRING.setLength(40));
    put("s_nationkey", DataType.IDENTIFIER);
    put("s_phone", DataType.FIXED_STRING.setLength(15));
    put("s_acctbal", DataType.FLOAT);
    put("s_comment", DataType.VARIABLE_STRING.setLength(101));
  }};
  public static Set<String> supplierKeysH = new HashSet<>(List.of("s_suppkey"));
  public static Integer supplierScaleFactorH = 10000;

  public static Map<String, DataType> partSuppTableH = new HashMap<>() {{
    put("ps_partkey", DataType.IDENTIFIER);
    put("ps_suppkey", DataType.IDENTIFIER);
    put("ps_availqty", DataType.INTEGER);
    put("ps_supplycost", DataType.FLOAT);
    put("ps_comment", DataType.VARIABLE_STRING.setLength(199));
  }};
  public static Set<String> partSuppKeysH = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));
  public static Integer partSuppScaleFactorH = 1;

  public static Map<String, DataType> customerTableH = new HashMap<>() {{
    put("c_custkey", DataType.IDENTIFIER);
    put("c_name", DataType.VARIABLE_STRING.setLength(25));
    put("c_address", DataType.VARIABLE_STRING.setLength(40));
    put("c_nationkey", DataType.IDENTIFIER);
    put("c_phone", DataType.FIXED_STRING.setLength(15));
    put("c_acctbal", DataType.FLOAT);
    put("c_mktsegment", DataType.FIXED_STRING.setLength(10));
    put("c_comment", DataType.VARIABLE_STRING.setLength(117));
  }};
  public static Set<String> customerKeysH = new HashSet<>(List.of("c_custkey"));
  public static Integer customerScaleFactorH = 150000;

  public static Map<String, DataType> ordersTableH = new HashMap<>() {{
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
  public static Set<String> ordersKeysH = new HashSet<>(List.of("o_orderkey"));
  public static Integer ordersScaleFactorH = 1500000;

  public static Map<String, DataType> lineItemTableH = new HashMap<>() {{
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
  public static Set<String> lineItemKeysH = new HashSet<>(List.of("l_orderkey", "l_linenumber"));
  public static Integer lineItemScaleFactorH = 1;

  public static Map<String, DataType> nationTableH = new HashMap<>() {{
    put("n_nationkey", DataType.IDENTIFIER);
    put("n_name", DataType.FIXED_STRING.setLength(25));
    put("n_regionkey", DataType.IDENTIFIER);
    put("n_comment", DataType.VARIABLE_STRING.setLength(152));
  }};
  public static Set<String> nationKeysH = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));
  public static Integer nationScaleFactorH = 1;

  public static Map<String, DataType> regionTableH = new HashMap<>() {{
    put("r_regionkey", DataType.IDENTIFIER);
    put("r_name", DataType.FIXED_STRING.setLength(25));
    put("r_comment", DataType.VARIABLE_STRING.setLength(152));
  }};
  public static Set<String> regionKeysH = new HashSet<>(List.of("ps_partkey", "ps_suppkey"));
  public static Integer regionScaleFactorH = 1;

  public static Map<String, DataType> warehouseTableC = new HashMap<>() {{
    put("w_id", DataType.IDENTIFIER);
    put("w_name", DataType.VARIABLE_STRING.setLength(10));
    put("w_street_1", DataType.VARIABLE_STRING.setLength(20));
    put("w_street_2", DataType.VARIABLE_STRING.setLength(20));
    put("w_city", DataType.VARIABLE_STRING.setLength(20));
    put("w_state", DataType.FIXED_STRING.setLength(2));
    put("w_zip", DataType.FIXED_STRING.setLength(9));
    put("w_tax", DataType.FLOAT);
    put("w_ytd", DataType.FLOAT);
  }};
  public static Set<String> warehouseKeysC = new HashSet<>(List.of("w_id"));
  public static Integer warehouseScaleFactorC = 2;

  public static Map<String, DataType> districtTableC = new HashMap<>() {{
    put("d_id", DataType.IDENTIFIER);
    put("d_w_id", DataType.IDENTIFIER);
    put("d_name", DataType.VARIABLE_STRING.setLength(10));
    put("d_street_1", DataType.VARIABLE_STRING.setLength(20));
    put("d_street_2", DataType.VARIABLE_STRING.setLength(20));
    put("d_city", DataType.VARIABLE_STRING.setLength(20));
    put("d_state", DataType.FIXED_STRING.setLength(2));
    put("d_zip", DataType.FIXED_STRING.setLength(9));
    put("d_tax", DataType.FLOAT);
    put("d_ytd", DataType.FLOAT);
    put("d_next_o_id", DataType.FLOAT);
  }};
  public static Set<String> districtKeysC = new HashSet<>(List.of("d_w_id", "d_id"));
  public static Integer districtScaleFactorC = 10;

  public static Map<String, DataType> customerTableC = new HashMap<>() {{
    put("c_id", DataType.IDENTIFIER);
    put("c_d_id", DataType.IDENTIFIER);
    put("c_w_id", DataType.IDENTIFIER);
    put("c_first", DataType.VARIABLE_STRING.setLength(16));
    put("c_middle", DataType.FIXED_STRING.setLength(2));
    put("c_last", DataType.VARIABLE_STRING.setLength(16));
    put("c_street_1", DataType.VARIABLE_STRING.setLength(20));
    put("c_street_2", DataType.VARIABLE_STRING.setLength(20));
    put("c_city", DataType.VARIABLE_STRING.setLength(20));
    put("c_state", DataType.FIXED_STRING.setLength(2));
    put("c_zip", DataType.FIXED_STRING.setLength(9));
    put("c_phone", DataType.FIXED_STRING.setLength(16));
    put("c_since", DataType.DATE);
    put("c_credit", DataType.FIXED_STRING.setLength(2));
    put("c_credit_lim", DataType.FLOAT);
    put("c_discount", DataType.FLOAT);
    put("c_balance", DataType.FLOAT);
    put("c_ytd_payment", DataType.FLOAT);
    put("c_payment_cnt", DataType.INTEGER);
    put("c_delivery_cnt", DataType.INTEGER);
    put("c_data", DataType.VARIABLE_STRING.setLength(500));
  }};
  public static Set<String> customerKeysC = new HashSet<>(List.of("c_w_id", "c_d_id", "c_id"));
  public static Integer customerScaleFactorC = 3000;

  public static Map<String, DataType> historyTableC = new HashMap<>() {{
    put("h_c_id", DataType.IDENTIFIER);
    put("h_c_d_id", DataType.IDENTIFIER);
    put("h_c_w_id", DataType.IDENTIFIER);
    put("h_d_id", DataType.IDENTIFIER);
    put("h_w_id", DataType.IDENTIFIER);
    put("h_date", DataType.DATE);
    put("h_amount", DataType.FLOAT);
    put("h_data", DataType.VARIABLE_STRING.setLength(24));
  }};
  public static Set<String> historyKeysC = new HashSet<>(List.of("h_c_w_id", "h_c_d_id", "h_c_id"));
  public static Integer historyScaleFactorC = 96000;

  public static Map<String, DataType> newOrderTableC = new HashMap<>() {{
    put("no_o_id", DataType.IDENTIFIER);
    put("no_d_id", DataType.IDENTIFIER);
    put("no_w_id", DataType.IDENTIFIER);
  }};
  public static Set<String> newOrderKeysC = new HashSet<>(List.of("no_w_id", "no_d_id", "no_o_id"));
  public static Integer newOrderScaleFactorC = 1;

  public static Map<String, DataType> orderTableC = new HashMap<>() {{
    put("o_id", DataType.IDENTIFIER);
    put("o_d_id", DataType.IDENTIFIER);
    put("o_w_id", DataType.IDENTIFIER);
    put("o_c_id", DataType.IDENTIFIER);
    put("o_entry_d", DataType.DATE);
    put("o_carrier_id", DataType.IDENTIFIER);
    put("o_ol_cnt", DataType.INTEGER);
    put("o_all_local", DataType.INTEGER);
  }};
  public static Set<String> orderKeysC = new HashSet<>(List.of("o_w_id", "o_d_id", "o_id"));
  public static Integer orderScaleFactorC = 10000000;

  public static Map<String, DataType> orderLineTableC = new HashMap<>() {{
    put("ol_o_id", DataType.IDENTIFIER);
    put("ol_d_id", DataType.IDENTIFIER);
    put("ol_w_id", DataType.IDENTIFIER);
    put("ol_number", DataType.IDENTIFIER);
    put("ol_i_id", DataType.IDENTIFIER);
    put("ol_supply_w_id", DataType.IDENTIFIER);
    put("ol_delivery_d", DataType.DATE);
    put("ol_quantity", DataType.INTEGER);
    put("ol_amount", DataType.FLOAT);
    put("ol_dist_info", DataType.FIXED_STRING.setLength(24));
  }};
  public static Set<String> orderLineKeysC = new HashSet<>(List.of("ol_w_id", "ol_d_id", "ol_o_id", "ol_number"));
  public static Integer orderLineScaleFactorC = 10000000;

  public static Map<String, DataType> itemTableC = new HashMap<>() {{
    put("i_id", DataType.IDENTIFIER);
    put("i_im_id", DataType.IDENTIFIER);
    put("i_name", DataType.VARIABLE_STRING.setLength(24));
    put("i_price", DataType.FLOAT);
    put("i_data", DataType.VARIABLE_STRING.setLength(50));
  }};
  public static Set<String> itemKeysC = new HashSet<>(List.of("i_id"));
  public static Integer itemScaleFactorC = 200000;

  public static Map<String, DataType> stockTableC = new HashMap<>() {{
    put("s_i_id", DataType.IDENTIFIER);
    put("s_w_id", DataType.IDENTIFIER);
    put("s_quantity", DataType.INTEGER);
    put("s_dist_01", DataType.FIXED_STRING.setLength(24));
    put("s_dist_02", DataType.FIXED_STRING.setLength(24));
    put("s_dist_03", DataType.FIXED_STRING.setLength(24));
    put("s_dist_04", DataType.FIXED_STRING.setLength(24));
    put("s_dist_05", DataType.FIXED_STRING.setLength(24));
    put("s_dist_06", DataType.FIXED_STRING.setLength(24));
    put("s_dist_07", DataType.FIXED_STRING.setLength(24));
    put("s_dist_08", DataType.FIXED_STRING.setLength(24));
    put("s_dist_09", DataType.FIXED_STRING.setLength(24));
    put("s_dist_10", DataType.FIXED_STRING.setLength(24));
    put("s_ytd", DataType.INTEGER);
    put("s_order_cnt", DataType.INTEGER);
    put("s_remote_cnt", DataType.INTEGER);
    put("s_data", DataType.VARIABLE_STRING.setLength(50));
  }};
  public static Set<String> stockKeysC = new HashSet<>(List.of("s_w_id", "s_i_id"));
  public static Integer stockScaleFactorC = 200000;
}
