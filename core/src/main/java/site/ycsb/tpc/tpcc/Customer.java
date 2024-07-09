package site.ycsb.tpc.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.tpc.TPCCUtil;

public class Customer {
  private final ObjectNode data;

  public Customer(int c_id, int c_d_id, int c_w_id, double c_balance, TPCCUtil util) {
    String c_first = util.makeAlphaString(8, 16);
    String c_middle = "O" + "E";

    String c_last;
    if (c_id <= 1000) {
      c_last = util.lastName(c_id - 1);
    } else {
      c_last = util.lastName(util.nuRand(255, 0, 999));
    }

    String c_street_1 = util.makeAlphaString(10, 20);
    String c_street_2 = util.makeAlphaString(10, 20);
    String c_city = util.makeAlphaString(10, 20);
    String c_state = util.makeAlphaString(2, 2);
    String c_zip = util.makeAlphaString(9, 9);

    String c_phone = util.makeNumberString(16, 16);

    String c_credit;
    if (util.randomNumber(0, 1) == 1)
      c_credit = "G";
    else
      c_credit = "B";
    c_credit += "C";

    int c_credit_lim = 50000;
    float c_discount = (float) (((float) util.randomNumber(0, 50)) / 100.0);

    double c_ytd_payment = 10.0;
    c_balance = c_balance - c_ytd_payment;

    String c_data = util.makeAlphaString(300, 500);
    String date = util.startDateText();

    ObjectMapper mapper = new ObjectMapper();
    this.data = mapper.createObjectNode();
    this.data.put("c_id", c_id);
    this.data.put("c_d_id", c_d_id);
    this.data.put("c_w_id", c_w_id);
    this.data.put("c_first", c_first);
    this.data.put("c_middle", c_middle);
    this.data.put("c_last", c_last);
    this.data.put("c_street_1", c_street_1);
    this.data.put("c_street_2", c_street_2);
    this.data.put("c_city", c_city);
    this.data.put("c_state", c_state);
    this.data.put("c_zip", c_zip);
    this.data.put("c_phone", c_phone);
    this.data.put("c_since", date);
    this.data.put("c_credit", c_credit);
    this.data.put("c_credit_lim", c_credit_lim);
    this.data.put("c_discount", c_discount);
    this.data.put("c_balance", c_balance);
    this.data.put("c_ytd_payment", c_ytd_payment);
    this.data.put("c_payment_cnt", 1);
    this.data.put("c_delivery_cnt", 0);
    this.data.put("c_data", c_data);
  }

  public int c_id() {
    return data.get("c_id").asInt();
  }

  public int c_d_id() {
    return data.get("c_d_id").asInt();
  }

  public int c_w_id() {
    return data.get("c_w_id").asInt();
  }

  public String c_first() {
    return data.get("c_first").asText();
  }

  public String c_middle() {
    return data.get("c_middle").asText();
  }

  public String c_last() {
    return data.get("c_last").asText();
  }

  public String c_street_1() {
    return data.get("c_street_1").asText();
  }

  public String c_street_2() {
    return data.get("c_street_2").asText();
  }

  public String c_city() {
    return data.get("c_city").asText();
  }

  public String c_state() {
    return data.get("c_state").asText();
  }

  public String c_zip() {
    return data.get("c_zip").asText();
  }

  public String c_phone() {
    return data.get("c_phone").asText();
  }

  public String c_since() {
    return data.get("c_since").asText();
  }

  public String c_credit() {
    return data.get("c_credit").asText();
  }

  public int c_credit_lim() {
    return data.get("c_credit_lim").asInt();
  }

  public double c_discount() {
    return data.get("c_discount").asDouble();
  }

  public double c_balance() {
    return data.get("c_balance").asDouble();
  }

  public double c_ytd_payment() {
    return data.get("c_ytd_payment").asDouble();
  }

  public int c_payment_cnt() {
    return data.get("c_payment_cnt").asInt();
  }

  public int c_delivery_cnt() {
    return data.get("c_delivery_cnt").asInt();
  }

  public String c_data() {
    return data.get("c_data").asText();
  }

  public ObjectNode asNode() {
    return data;
  }

  public String asJson() {
    return data.toString();
  }
}
