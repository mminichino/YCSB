package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

public class Stock {
  private final ObjectNode data;

  public Stock(int s_i_id, int s_w_id, int warehouseCount, TPCCUtil util, int[] orig) {
    int s_quantity = util.randomNumber(10, 100);

    String s_dist_01 = util.makeAlphaString(24, 24);
    String s_dist_02 = util.makeAlphaString(24, 24);
    String s_dist_03 = util.makeAlphaString(24, 24);
    String s_dist_04 = util.makeAlphaString(24, 24);
    String s_dist_05 = util.makeAlphaString(24, 24);
    String s_dist_06 = util.makeAlphaString(24, 24);
    String s_dist_07 = util.makeAlphaString(24, 24);
    String s_dist_08 = util.makeAlphaString(24, 24);
    String s_dist_09 = util.makeAlphaString(24, 24);
    String s_dist_10 = util.makeAlphaString(24, 24);

    String s_data = util.makeAlphaString(26, 50);

    if (orig[s_i_id] != 0) {
      s_data = "original";
    }

    int s_order_cnt = util.randomNumber(10, 3000);

    int s_remote_cnt;
    if (warehouseCount > 1) {
      s_remote_cnt = (int) Math.round(s_order_cnt * 0.1);
    } else {
      s_remote_cnt = 0;
    }

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("s_i_id", s_i_id);
    this.data.put("s_w_id", s_w_id);
    this.data.put("s_quantity", s_quantity);
    this.data.put("s_dist_01", s_dist_01);
    this.data.put("s_dist_02", s_dist_02);
    this.data.put("s_dist_03", s_dist_03);
    this.data.put("s_dist_04", s_dist_04);
    this.data.put("s_dist_05", s_dist_05);
    this.data.put("s_dist_06", s_dist_06);
    this.data.put("s_dist_07", s_dist_07);
    this.data.put("s_dist_08", s_dist_08);
    this.data.put("s_dist_09", s_dist_09);
    this.data.put("s_dist_10", s_dist_10);
    this.data.put("s_ytd", 0);
    this.data.put("s_order_cnt", s_order_cnt);
    this.data.put("s_remote_cnt", s_remote_cnt);
    this.data.put("s_data", s_data);
  }

  public int s_i_id() {
    return data.get("s_i_id").asInt();
  }

  public int s_w_id() {
    return data.get("s_w_id").asInt();
  }

  public int s_quantity() {
    return data.get("s_quantity").asInt();
  }

  public String s_dist_01() {
    return data.get("s_dist_01").asText();
  }

  public String s_dist_02() {
    return data.get("s_dist_02").asText();
  }

  public String s_dist_03() {
    return data.get("s_dist_03").asText();
  }

  public String s_dist_04() {
    return data.get("s_dist_04").asText();
  }

  public String s_dist_05() {
    return data.get("s_dist_05").asText();
  }

  public String s_dist_06() {
    return data.get("s_dist_06").asText();
  }

  public String s_dist_07() {
    return data.get("s_dist_07").asText();
  }

  public String s_dist_08() {
    return data.get("s_dist_08").asText();
  }

  public String s_dist_09() {
    return data.get("s_dist_09").asText();
  }

  public String s_dist_10() {
    return data.get("s_dist_10").asText();
  }

  public int s_ytd() {
    return data.get("s_ytd").asInt();
  }

  public int s_order_cnt() {
    return data.get("s_order_cnt").asInt();
  }

  public String s_remote_cnt() {
    return data.get("s_remote_cnt").asText();
  }

  public String s_data() {
    return data.get("s_data").asText();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
