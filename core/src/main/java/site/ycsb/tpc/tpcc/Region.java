package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.NationData;
import site.ycsb.tpc.TPCCUtil;

public class Region {
  private final ObjectNode data;

  public Region(int r_regionkey, TPCCUtil util) {
    String r_name = util.getRegion(r_regionkey);
    String r_comment = util.makeAlphaString(31, 115);

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("r_regionkey", r_regionkey);
    this.data.put("r_name", r_name);
    this.data.put("r_comment", r_comment);
  }

  public int r_regionkey() {
    return data.get("r_regionkey").asInt();
  }

  public String r_name() {
    return data.get("r_name").asText();
  }

  public int r_comment() {
    return data.get("r_comment").asInt();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
