package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.NationData;
import site.ycsb.tpc.TPCCUtil;

public class Nation {
  private final ObjectNode data;

  public Nation(int nationkey, TPCCUtil util) {
    NationData nation = util.getNation(nationkey);

    int n_nationkey = nation.id;
    String n_name = nation.name;
    int n_regionkey = nation.regionId;
    String n_comment = util.makeAlphaString(31, 114);

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("n_nationkey", n_nationkey);
    this.data.put("n_name", n_name);
    this.data.put("n_regionkey", n_regionkey);
    this.data.put("n_comment", n_comment);
  }

  public int n_nationkey() {
    return data.get("n_nationkey").asInt();
  }

  public String n_name() {
    return data.get("n_name").asText();
  }

  public int n_regionkey() {
    return data.get("n_regionkey").asInt();
  }

  public String n_comment() {
    return data.get("n_comment").asText();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
