package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

import java.util.List;

public class Supplier {
  private final ObjectNode data;

  public Supplier(int su_suppkey, List<Integer> comments, List<Integer> complaints, int[] nationVector, TPCCUtil util) {
    String su_name = "Supplier#" + util.strLeadingZero(su_suppkey, 9);
    String su_address = util.makeRandomString(10, 40);

    int su_nationkey;
    while (true) {
      int nationKey = util.getNation(util.randomNumber(1, util.numNations())).id;
      int nationKeyIndex = util.nationIndex(nationKey);
      if (nationVector[nationKeyIndex] <= 162) {
        su_nationkey = nationKey;
        nationVector[nationKeyIndex]++;
        break;
      }
    }

    String su_phone = util.makeNumberString(16, 16);
    double su_acctbal = util.randomDouble(-999.99, 9999.99,2);
    String su_comment;
    if (comments.contains(su_suppkey)) {
      su_comment = util.tpchTextStringCustomer(25, 100, "Recommends");
    } else if (complaints.contains(su_suppkey)) {
      su_comment = util.tpchTextStringCustomer(25, 100, "Complaints");
    } else {
      su_comment = util.tpchTextString(25, 100);
    }

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("su_suppkey", su_suppkey);
    this.data.put("su_name", su_name);
    this.data.put("su_address", su_address);
    this.data.put("su_nationkey", su_nationkey);
    this.data.put("su_phone", su_phone);
    this.data.put("su_acctbal", su_acctbal);
    this.data.put("su_comment", su_comment);
  }

  public int su_suppkey() {
    return data.get("su_suppkey").asInt();
  }

  public String su_name() {
    return data.get("su_name").asText();
  }

  public String su_address() {
    return data.get("su_address").asText();
  }

  public int su_nationkey() {
    return data.get("su_nationkey").asInt();
  }

  public String su_phone() {
    return data.get("su_phone").asText();
  }

  public double su_acctbal() {
    return data.get("su_acctbal").asDouble();
  }

  public String su_comment() {
    return data.get("su_comment").asText();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
