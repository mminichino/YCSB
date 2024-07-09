package site.ycsb.tpc;

import site.ycsb.SQLDB;
import site.ycsb.Record;
import site.ycsb.Status;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;

public final class TPCCLoad {
  static final Logger LOGGER =
      (Logger)LoggerFactory.getLogger("site.ycsb.tpc.TPCCLoad");
  private final int maxItems;
  private final int custPerDist;
  private final int distPerWarehouse;
  private final int ordPerDist;
  private final boolean enableDebug;
  private final TPCCUtil util;
  private final ExecutorService executor;

  public static class TPCCLoadBuilder {
    private int maxItems = 100000;
    private int custPerDist = 3000;
    private int distPerWarehouse = 10;
    private int ordPerDist = 3000;
    private boolean enableDebug = false;
    private int threadCount = 32;

    public TPCCLoadBuilder maxItems(int value) {
      this.maxItems = value;
      return this;
    }

    public TPCCLoadBuilder custPerDist(int value) {
      this.custPerDist = value;
      return this;
    }

    public TPCCLoadBuilder distPerWarehouse(int value) {
      this.distPerWarehouse = value;
      return this;
    }

    public TPCCLoadBuilder ordPerDist(int value) {
      this.ordPerDist = value;
      return this;
    }

    public TPCCLoadBuilder enableDebug(boolean value) {
      this.enableDebug = value;
      return this;
    }

    public TPCCLoadBuilder threadCount(int value) {
      this.threadCount = value;
      return this;
    }

    public TPCCLoad build() {
      return new TPCCLoad(this);
    }
  }

  private TPCCLoad(TPCCLoadBuilder builder) {
    this.maxItems = builder.maxItems;
    this.custPerDist = builder.custPerDist;
    this.distPerWarehouse = builder.distPerWarehouse;
    this.ordPerDist = builder.ordPerDist;
    this.enableDebug = builder.enableDebug;
    this.util = new TPCCUtil(custPerDist, ordPerDist);
    this.executor = Executors.newFixedThreadPool(builder.threadCount);
  }

  public void loadItems(Queue<Record> queue) {
    int[] orig = new int[maxItems + 1];
    int pos;
    int i;

    System.out.println("Loading Item Table");

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }
    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = util.randomNumber(0, maxItems);
      } while (orig[pos] != 0);
      orig[pos] = 1;
    }

    for (int i_id = 1; i_id <= maxItems; i_id++) {
      int i_im_id = util.randomNumber(1, 10000);

      String i_name = util.makeAlphaString(14, 24);

      float i_price = (float) (util.randomNumber(100, 10000) / 100.0);

      String i_data = util.makeAlphaString(26, 50);

      if (orig[i_id] != 0) {
        int position = util.randomNumber(0, i_data.length() - 8);
        i_data = i_data.substring(0, position) + "original" + i_data.substring(position + 8);
      }

      Record record = new Record("item");
      record.setKey(Tables.itemKeysC);
      record.add("i_id", i_id);
      record.add("i_im_id", i_im_id);
      record.add("i_name", i_name);
      record.add("i_price", i_price);
      record.add("i_data", i_data);
      queue.offer(record);
    }

    System.out.println("Item Table Load Complete");
  }

  public void loadWare(Queue<Record> queue, int max_ware) {
    System.out.println("Loading Warehouse Table");

    for (int w_id = 1; w_id <= max_ware; w_id++) {
      String w_name = util.makeAlphaString(6, 10);
      String w_street_1 = util.makeAlphaString(10, 20);
      String w_street_2 = util.makeAlphaString(10, 20);
      String w_city = util.makeAlphaString(10, 20);
      String w_state = util.makeAlphaString(2, 2);
      String w_zip = util.makeAlphaString(9, 9);

      double w_tax = ((double) util.randomNumber(10, 20) / 100.0);
      double w_ytd = 3000000.00;

      if (enableDebug) {
        System.out.printf("WID = %d, Name = %s, Tax = %f\n", w_id, w_name, w_tax);
      }

      Record record = new Record("warehouse");
      record.setKey(Tables.warehouseKeysC);
      record.add("w_id", w_id);
      record.add("w_name", w_name);
      record.add("w_street_1", w_street_1);
      record.add("w_street_2", w_street_2);
      record.add("w_city", w_city);
      record.add("w_state", w_state);
      record.add("w_zip", w_zip);
      record.add("w_tax", w_tax);
      record.add("w_ytd", w_ytd);
      queue.offer(record);

      stock(queue, w_id);
      district(queue, w_id);
    }
    System.out.println("Warehouse Table Load Complete");
  }

  public void loadCust(Queue<Record> queue, int max_ware) {
    try {
      for (int w_id = 1; w_id <= max_ware; w_id++) {
        for (int d_id = 1; d_id <= distPerWarehouse; d_id++) {
          loadCustomer(queue, d_id, w_id);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error loading customers", e);
    }
  }

  public void loadOrd(Queue<Record> queue, int max_ware) {
    try {
      for (int w_id = 1; w_id <= max_ware; w_id++) {
        for (int d_id = 1; d_id <= distPerWarehouse; d_id++) {
          loadOrders(queue, d_id, w_id);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error loading orders", e);
    }
  }

  public void stock(Queue<Record> queue, int s_w_id) {
    int[] orig = new int[maxItems + 1];
    int pos;
    int i;

    System.out.printf("Loading Stock Wid=%d\n", s_w_id);

    for (i = 0; i < maxItems / 10; i++) {
      orig[i] = 0;
    }

    for (i = 0; i < maxItems / 10; i++) {
      do {
        pos = util.randomNumber(0, maxItems);
      } while (orig[pos] != 0); //TODO: FIx later
      orig[pos] = 1;
    }

    for (int s_i_id = 1; s_i_id <= maxItems; s_i_id++) {
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

      Record record = new Record("stock");
      record.setKey(Tables.stockKeysC);
      record.add("s_i_id", s_i_id);
      record.add("s_w_id", s_w_id);
      record.add("s_quantity", s_quantity);
      record.add("s_dist_01", s_dist_01);
      record.add("s_dist_02", s_dist_02);
      record.add("s_dist_03", s_dist_03);
      record.add("s_dist_04", s_dist_04);
      record.add("s_dist_05", s_dist_05);
      record.add("s_dist_06", s_dist_06);
      record.add("s_dist_07", s_dist_07);
      record.add("s_dist_08", s_dist_08);
      record.add("s_dist_09", s_dist_09);
      record.add("s_dist_10", s_dist_10);
      record.add("s_ytd", 0);
      record.add("s_order_cnt", 0);
      record.add("s_remote_cnt", 0);
      record.add("s_data", s_data);
      queue.offer(record);

      if (enableDebug)
        System.out.printf("SID = %d, WID = %d, Quan = %d\n", s_i_id, s_w_id, s_quantity);
    }

    System.out.println("Stock Done");
  }

  public void district(Queue<Record> queue, int d_w_id) {
    float d_ytd = (float) 30000.0;
    int d_next_o_id = 3001;

    System.out.println("Loading District");

    for (int d_id = 1; d_id <= distPerWarehouse; d_id++) {
      String d_name = util.makeAlphaString(6, 10);
      String d_street_1 = util.makeAlphaString(10, 20);
      String d_street_2 = util.makeAlphaString(10, 20);
      String d_city = util.makeAlphaString(10, 20);
      String d_state = util.makeAlphaString(2, 2);
      String d_zip = util.makeAlphaString(9, 9);

      float d_tax = (float) (((float) util.randomNumber(10, 20)) / 100.0);

      Record record = new Record("district");
      record.setKey(Tables.districtKeysC);
      record.add("d_id", d_id);
      record.add("d_w_id", d_w_id);
      record.add("d_name", d_name);
      record.add("d_street_1", d_street_1);
      record.add("d_street_2", d_street_2);
      record.add("d_city", d_city);
      record.add("d_state", d_state);
      record.add("d_zip", d_zip);
      record.add("d_tax", d_tax);
      record.add("d_ytd", d_ytd);
      record.add("d_next_o_id", d_next_o_id);
      queue.offer(record);

      if (enableDebug)
        System.out.printf("DID = %d, WID = %d, Name = %s, Tax = %f\n", d_id, d_w_id, d_name, d_tax);
    }
  }

  public void loadCustomer(Queue<Record> queue, int c_d_id, int c_w_id) {
    System.out.printf("Loading Customer for DID=%d, WID=%d\n", c_d_id, c_w_id);

    for (int c_id = 1; c_id <= custPerDist; c_id++) {
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
      float c_balance = (float) -10.0;

      String c_data = util.makeAlphaString(300, 500);

      String dateFormat = "yy-MM-dd'T'HH:mm:ss";
      SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
      String date = timeStampFormat.format(new Date());

      try {
        Record record = new Record("customer");
        record.setKey(Tables.customerKeysC);
        record.add("c_id", c_id);
        record.add("c_d_id", c_d_id);
        record.add("c_w_id", c_w_id);
        record.add("c_first", c_first);
        record.add("c_middle", c_middle);
        record.add("c_last", c_last);
        record.add("c_street_1", c_street_1);
        record.add("c_street_2", c_street_2);
        record.add("c_city", c_city);
        record.add("c_state", c_state);
        record.add("c_zip", c_zip);
        record.add("c_phone", c_phone);
        record.add("date", date);
        record.add("c_credit", c_credit);
        record.add("c_credit_lim", c_credit_lim);
        record.add("c_discount", c_discount);
        record.add("c_balance", c_balance);
        record.add("c_ytd_payment", 10.0);
        record.add("c_payment_cnt", 1);
        record.add("c_delivery_cnt", 0);
        record.add("c_data", c_data);
        queue.offer(record);
      } catch (Exception e) {
        throw new RuntimeException("Customer insert error", e);
      }

      double h_amount = 10.0;
      String h_data = util.makeAlphaString(12, 24);

      try {
        Record record = new Record("history");
        record.setKey(Tables.historyKeysC);
        record.add("h_c_id", c_id);
        record.add("h_c_d_id", c_d_id);
        record.add("h_c_w_id", c_w_id);
        record.add("h_d_id", c_d_id);
        record.add("h_w_id", c_w_id);
        record.add("h_date", date);
        record.add("h_amount", h_amount);
        record.add("h_data", h_data);
        queue.offer(record);
      } catch (Exception e) {
        throw new RuntimeException("Insert into History error", e);
      }

      if (enableDebug)
        System.out.printf("CID = %d, LST = %s, P# = %s\n", c_id, c_last, c_phone);
    }
    System.out.println("Customer Done");
  }

  public void loadOrders(Queue<Record> queue, int o_d_id, int o_w_id) {
    System.out.printf("Loading Orders for D=%d, W=%d\n", o_d_id, o_w_id);

    util.initPermutation();

    for (int o_id = 1; o_id <= ordPerDist; o_id++) {
      int o_c_id = util.getPermutation();
      int o_carrier_id = util.randomNumber(1, 10);
      int o_ol_cnt = util.randomNumber(5, 15);

      String dateFormat = "yy-MM-dd'T'HH:mm:ss";
      SimpleDateFormat timeStampFormat = new SimpleDateFormat(dateFormat);
      String date = timeStampFormat.format(new Date());

      if (o_id > 2100) {
        Record record = new Record("orders");
        record.setKey(Tables.orderKeysC);
        record.add("o_id", o_id);
        record.add("o_d_id", o_d_id);
        record.add("o_w_id", o_w_id);
        record.add("o_c_id", o_c_id);
        record.add("o_entry_d", date);
        record.add("o_carrier_id");
        record.add("o_ol_cnt", o_ol_cnt);
        record.add("o_all_local", 1);
        queue.offer(record);

        record = new Record("new_orders");
        record.setKey(Tables.newOrderKeysC);
        record.add("no_o_id", o_id);
        record.add("no_d_id", o_d_id);
        record.add("no_w_id", o_w_id);
        queue.offer(record);
      } else {
        Record record = new Record("orders");
        record.setKey(Tables.orderKeysC);
        record.add("o_id", o_id);
        record.add("o_d_id", o_d_id);
        record.add("o_w_id", o_w_id);
        record.add("o_c_id", o_c_id);
        record.add("o_entry_d", date);
        record.add("o_carrier_id", o_carrier_id);
        record.add("o_ol_cnt", o_ol_cnt);
        record.add("o_all_local", 1);
        queue.offer(record);
      }

      if (enableDebug)
        System.out.printf("OID = %d, CID = %d, DID = %d, WID = %d\n",
            o_id, o_c_id, o_d_id, o_w_id);

      for (int ol = 1; ol <= o_ol_cnt; ol++) {
        int ol_i_id = util.randomNumber(1, maxItems);
        int ol_supply_w_id = o_w_id;
        int ol_quantity = 5;
        float ol_amount = (float) 0.0;

        String ol_dist_info = util.makeAlphaString(24, 24);

        float tmp_float = (float) ((float) (util.randomNumber(10, 10000)) / 100.0);

        if (o_id > 2100) {
          Record record = new Record("order_line");
          record.setKey(Tables.orderLineKeysC);
          record.add("ol_o_id", o_id);
          record.add("ol_d_id", o_d_id);
          record.add("ol_w_id", o_w_id);
          record.add("ol_number", ol);
          record.add("ol_i_id", ol_i_id);
          record.add("ol_supply_w_id", ol_supply_w_id);
          record.add("ol_delivery_d");
          record.add("ol_quantity", ol_quantity);
          record.add("ol_amount", ol_amount);
          record.add("ol_dist_info", ol_dist_info);
          queue.offer(record);
        } else {
          Record record = new Record("order_line");
          record.setKey(Tables.orderLineKeysC);
          record.add("ol_o_id", o_id);
          record.add("ol_d_id", o_d_id);
          record.add("ol_w_id", o_w_id);
          record.add("ol_number", ol);
          record.add("ol_i_id", ol_i_id);
          record.add("ol_supply_w_id", ol_supply_w_id);
          record.add("ol_delivery_d", date);
          record.add("ol_quantity", ol_quantity);
          record.add("ol_amount", tmp_float);
          record.add("ol_dist_info", ol_dist_info);
          queue.offer(record);
        }

        if (enableDebug) {
          System.out.printf("OL = %d, IID = %d, QUAN = %d, AMT = %f\n", ol, ol_i_id, ol_quantity, ol_amount);
        }
      }
    }
    System.out.println("Orders Done");
  }
}
