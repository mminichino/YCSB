package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

public class Warehouse {
  private final ObjectNode data;

  public Warehouse(int w_id, TPCCUtil util) {
    String w_name = util.makeAlphaString(6, 10);
    String w_street_1 = util.makeAlphaString(10, 20);
    String w_street_2 = util.makeAlphaString(10, 20);
    String w_city = util.makeAlphaString(10, 20);
    String w_state = util.makeAlphaString(2, 2);
    String w_zip = util.makeAlphaString(9, 9);

    double w_tax = ((double) util.randomNumber(10, 20) / 100.0);
    double w_ytd = 3000000.00;

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("w_id", w_id);
    this.data.put("w_name", w_name);
    this.data.put("w_street_1", w_street_1);
    this.data.put("w_street_2", w_street_2);
    this.data.put("w_city", w_city);
    this.data.put("w_state", w_state);
    this.data.put("w_zip", w_zip);
    this.data.put("w_tax", w_tax);
    this.data.put("w_ytd", w_ytd);
  }

  public String w_id() {
    return data.get("w_id").asText();
  }

  public String w_name() {
    return data.get("w_name").asText();
  }

  public String w_street_1() {
    return data.get("w_street_1").asText();
  }

  public String w_street_2() {
    return data.get("w_street_2").asText();
  }

  public String w_city() {
    return data.get("w_city").asText();
  }

  public String w_state() {
    return data.get("w_state").asText();
  }

  public String w_zip() {
    return data.get("w_zip").asText();
  }

  public double w_tax() {
    return data.get("w_tax").asDouble();
  }

  public double w_ytd() {
    return data.get("w_ytd").asDouble();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
