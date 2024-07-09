package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

public class District {
  private final ObjectNode data;

  public District(int d_id, int d_w_id, TPCCUtil util) {
    float d_ytd = (float) 30000.0;
    int d_next_o_id = 3001;

    String d_name = util.makeAlphaString(6, 10);
    String d_street_1 = util.makeAlphaString(10, 20);
    String d_street_2 = util.makeAlphaString(10, 20);
    String d_city = util.makeAlphaString(10, 20);
    String d_state = util.makeAlphaString(2, 2);
    String d_zip = util.makeAlphaString(9, 9);

    float d_tax = (float) (((float) util.randomNumber(10, 20)) / 100.0);

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("d_id", d_id);
    this.data.put("d_w_id", d_w_id);
    this.data.put("d_name", d_name);
    this.data.put("d_street_1", d_street_1);
    this.data.put("d_street_2", d_street_2);
    this.data.put("d_city", d_city);
    this.data.put("d_state", d_state);
    this.data.put("d_zip", d_zip);
    this.data.put("d_tax", d_tax);
    this.data.put("d_ytd", d_ytd);
    this.data.put("d_next_o_id", d_next_o_id);
  }

  public int d_id() {
    return data.get("d_id").asInt();
  }

  public int d_w_id() {
    return data.get("d_w_id").asInt();
  }

  public String d_name() {
    return data.get("d_name").asText();
  }

  public String d_street_1() {
    return data.get("d_street_1").asText();
  }

  public String d_street_2() {
    return data.get("d_street_2").asText();
  }

  public String d_city() {
    return data.get("d_city").asText();
  }

  public String d_state() {
    return data.get("d_state").asText();
  }

  public String d_zip() {
    return data.get("d_zip").asText();
  }

  public double d_tax() {
    return data.get("d_tax").asDouble();
  }

  public double d_ytd() {
    return data.get("d_ytd").asDouble();
  }

  public int d_next_o_id() {
    return data.get("d_next_o_id").asInt();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
