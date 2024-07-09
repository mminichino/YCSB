package site.ycsb.tpc;

import java.util.ArrayList;

public class TPCCStatements {
  private final String[] pStmts = new String[37];

  public void TpccStatements() {
    // NewOrder statements.
    pStmts[0] = "SELECT c.c_discount, c.c_last, c.c_credit, w.w_tax FROM customer AS c JOIN warehouse AS w ON c.c_w_id = w.w_id AND w.w_id = ? AND c.c_w_id = ? AND c.c_d_id = ? AND c.c_id = ?";
    pStmts[1] = "SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ?";
    pStmts[2] = "UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?";
    pStmts[3] = "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)";
    pStmts[4] = "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)";
    pStmts[5] = "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?";
    pStmts[6] = "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ?";
    pStmts[7] = "UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?";
    pStmts[8] = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    // Payment statements.
    pStmts[9] = "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?";
    pStmts[10] = "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?";
    pStmts[11] = "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?";
    pStmts[12] = "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?";
    pStmts[13] = "SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?";
    pStmts[14] = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
    pStmts[15] = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE";
    pStmts[16] = "SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
    pStmts[17] = "UPDATE customer SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
    pStmts[18] = "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
    pStmts[19] = "INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

    // OrderStat statements.
    pStmts[20] = "SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?";
    pStmts[21] = "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
    pStmts[22] = "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
    pStmts[23] = "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)";
    pStmts[24] = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";

    // Delivery statements.
    pStmts[25] = "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?";
    pStmts[26] = "DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?";
    pStmts[27] = "SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?";
    pStmts[28] = "UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?";
    pStmts[29] = "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?";
    pStmts[30] = "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?";
    pStmts[31] = "UPDATE customer SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?";

    // Slev statements.
    pStmts[32] = "SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?";
    pStmts[33] = "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)";
    pStmts[34] = "SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?";

    // These are used in place of pStmts[0] in order to avoid joins
    pStmts[35] = "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?";
    pStmts[36] = "SELECT w_tax FROM warehouse WHERE w_id = ?";
  }

  public String getStatement(int idx) {
    return pStmts[idx];
  }
}
