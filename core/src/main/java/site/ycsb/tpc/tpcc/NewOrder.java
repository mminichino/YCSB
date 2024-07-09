package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NewOrder {
  private final ObjectNode data;

  public NewOrder(int o_id, int o_d_id, int o_w_id) {
    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("no_o_id", o_id);
    this.data.put("no_d_id", o_d_id);
    this.data.put("no_w_id", o_w_id);
  }

  public int no_o_id() {
    return data.get("no_o_id").asInt();
  }

  public int no_d_id() {
    return data.get("no_d_id").asInt();
  }

  public int no_w_id() {
    return data.get("no_w_id").asInt();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
