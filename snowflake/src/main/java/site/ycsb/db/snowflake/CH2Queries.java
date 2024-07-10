package site.ycsb.db.snowflake;

import site.ycsb.BenchQueries;

public class CH2Queries extends BenchQueries {
  public static final String[] sqlStatements = {
      // Q01
      "SELECT ol.ol_number ," +
          "SUM(ol.ol_quantity) as sum_qty ," +
          "SUM(ol.ol_amount) as sum_amount ," +
          "AVG(ol.ol_quantity) as avg_qty ," +
          "AVG(ol.ol_amount) as avg_amount ," +
          "COUNT(*) as COUNT_order " +
          "FROM order_line ol " +
          "WHERE ol.ol_delivery_d > '2014-07-01 00:00:00' " +
          "GROUP BY ol.ol_number " +
          "ORDER BY ol.ol_number",
      // Q02
      "SELECT su.su_suppkey, su.su_name, n.n_name, i.i_id, i.i_name, su.su_address, su.su_phone, su.su_comment " +
          "FROM (SELECT s1.s_i_id as m_i_id, MIN(s1.s_quantity) as m_s_quantity " +
          "FROM stock s1, " +
          "(SELECT su1.su_suppkey " +
          "FROM supplier su1, (SELECT n1.n_nationkey from nation n1, region r1 " +
          "WHERE n1.n_regionkey=r1.r_regionkey AND r1.r_name LIKE 'Europ%') t1 " +
          "WHERE su1.su_nationkey=t1.n_nationkey) t2 " +
          "WHERE MOD(s1.s_w_id * s1.s_i_id, 10000) = t2.su_suppkey " +
          "GROUP BY s1.s_i_id) m,  item i, stock s, supplier su, nation n, region r " +
          "WHERE i.i_id = s.s_i_id " +
          "AND MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey " +
          "AND su.su_nationkey = n.n_nationkey " +
          "AND n.n_regionkey = r.r_regionkey " +
          "AND i.i_data LIKE '%b' " +
          "AND r.r_name LIKE 'Europ%' " +
          "AND i.i_id=m.m_i_id " +
          "AND s.s_quantity = m.m_s_quantity " +
          "ORDER BY n.n_name, su.su_name, i.i_id limit 100",
      // Q03
      "WITH co as " +
          "(SELECT o.o_id, o.o_w_id, o.o_d_id, o.o_entry_d, ol.ol_amount " +
          "FROM orders o, customer c, order_line ol " +
          "WHERE o.o_id = ol.ol_o_id AND c.c_state LIKE 'A%' " +
          "AND c.c_id = o.o_c_id AND c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id " +
          "AND o.o_entry_d < '2017-03-15 00:00:00.000000') " +
          "SELECT co.o_id, co.o_w_id, co.o_d_id, SUM(co.ol_amount) as revenue, co.o_entry_d " +
          "FROM co, new_orders no " +
          "WHERE no.no_w_id = co.o_w_id AND no.no_d_id = co.o_d_id AND no.no_o_id = co.o_id " +
          "GROUP BY co.o_id, co.o_w_id, co.o_d_id, co.o_entry_d " +
          "ORDER BY revenue DESC, co.o_entry_d",
      // Q04
      "SELECT o.o_ol_cnt, COUNT(*) as order_COUNT " +
          "FROM orders o, order_line ol " +
          "WHERE o.o_id = ol.ol_o_id " +
          "AND o.o_entry_d >= '2015-07-01 00:00:00.000000' " +
          "AND o.o_entry_d < '2015-10-01 00:00:00.000000' " +
          "AND ol.ol_delivery_d >= dateadd(week, 1, o.o_entry_d) " +
          "GROUP BY o.o_ol_cnt " +
          "ORDER BY o.o_ol_cnt ",
      // Q05
      "SELECT cnros.n_name, ROUND(sum (cnros.ol_amount),2) as revenue " +
          "FROM (SELECT cnro.ol_amount, cnro.n_name, cnro.n_nationkey, s.s_w_id, s.s_i_id " +
          "FROM stock s JOIN " +
          "(SELECT o.o_w_id, ol.ol_amount, ol.ol_i_id, cnr.n_name, cnr.n_nationkey " +
          "FROM orders o JOIN order_line ol ON o.o_id = ol.ol_o_id JOIN " +
          "(SELECT c.c_id, c.c_w_id, c.c_d_id, nr.n_name, nr.n_nationkey " +
          "FROM customer c JOIN " +
          "(SELECT n.n_nationkey, n.n_name " +
          "FROM nation n, region r " +
          "WHERE n.n_regionkey = r.r_regionkey AND r.r_name = 'Asia') nr " +
          "ON UNICODE(c.c_state) = nr.n_nationkey) cnr " +
          "ON o.o_entry_d >= '2016-01-01 00:00:00.000000' AND o.o_entry_d < '2017-01-01 00:00:00.000000' " +
          "AND cnr.c_id = o.o_c_id AND cnr.c_w_id = o.o_w_id AND cnr.c_d_id = o.o_d_id) cnro " +
          "ON cnro.o_w_id = s.s_w_id AND cnro.ol_i_id = s.s_i_id) cnros JOIN supplier su " +
          "ON MOD(cnros.s_w_id * cnros.s_i_id, 10000) = su.su_suppkey AND su.su_nationkey = cnros.n_nationkey " +
          "GROUP BY cnros.n_name " +
          "ORDER BY revenue DESC",
      // Q06
      "SELECT SUM(ol.ol_amount) as revenue " +
          "FROM   order_line ol " +
          "WHERE  ol.ol_delivery_d >= '2016-01-01 00:00:00.000000' " +
          "AND  ol.ol_delivery_d < '2017-01-01 00:00:00.000000' " +
          "AND  ol.ol_amount > 600",
      // Q07
      "SELECT su.su_nationkey as supp_nation, SUBSTR(n1n2cools.c_state,1,1) as cust_nation, DATE_PART(year, n1n2cools.o_entry_d) as l_year, ROUND(SUM(n1n2cools.ol_amount),2) as revenue " +
          "FROM " +
          "(select n1n2cool.c_state, n1n2cool.o_entry_d, n1n2cool.ol_amount, n1n2cool.n1key, s.s_w_id, s.s_i_id " +
          "FROM stock s JOIN " +
          "(SELECT o.o_entry_d, ol.ol_supply_w_id, ol.ol_i_id, n1n2c.c_state, ol.ol_amount, n1n2c.n1key " +
          "FROM orders o JOIN order_line ol ON o.o_id = ol.ol_o_id JOIN " +
          "(SELECT c.c_id, c.c_w_id, c.c_d_id, c.c_state, n1n2.n1key " +
          "FROM customer c JOIN " +
          "(SELECT n1.n_nationkey n1key, n2.n_nationkey n2key " +
          "FROM nation n1, nation n2 " +
          "WHERE (n1.n_name = 'Germany' AND n2.n_name = 'Cambodia') OR (n1.n_name = 'Cambodia' AND n2.n_name = 'Germany') " +
          ")n1n2 " +
          "ON UNICODE(c.c_state) = n1n2.n2key) n1n2c " +
          "ON n1n2c.c_id = o.o_c_id AND n1n2c.c_w_id = o.o_w_id AND n1n2c.c_d_id = o.o_d_id " +
          "AND ol.ol_delivery_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000') n1n2cool " +
          "ON n1n2cool.ol_supply_w_id = s.s_w_id AND n1n2cool.ol_i_id = s.s_i_id)  n1n2cools JOIN supplier su " +
          "ON MOD(n1n2cools.s_w_id * n1n2cools.s_i_id, 10000) = su.su_suppkey AND su.su_nationkey = n1n2cools.n1key " +
          "GROUP BY su.su_nationkey, SUBSTR(n1n2cools.c_state,1,1), DATE_PART(year, n1n2cools.o_entry_d) " +
          "ORDER BY su.su_nationkey, cust_nation, l_year",
      // Q08
      "SELECT DATE_PART(year, rn1coolis.o_entry_d) as l_year, " +
          "ROUND((SUM(case when sun2.n_name = 'Germany' then rn1coolis.ol_amount else 0 end) / SUM(rn1coolis.ol_amount)),2) as mkt_share " +
          "FROM " +
          "(SELECT rn1cooli.o_entry_d,  rn1cooli.ol_amount, s.s_w_id, s.s_i_id " +
          "FROM stock s JOIN " +
          "(SELECT o.o_entry_d, ol.ol_i_id, ol.ol_amount, ol.ol_supply_w_id " +
          "FROM orders o JOIN order_line ol ON o.o_id = ol.ol_o_id JOIN item i JOIN " +
          "(SELECT c.c_id, c.c_w_id, c.c_d_id " +
          "FROM customer c JOIN " +
          "(SELECT n1.n_nationkey " +
          "FROM nation n1, region r " +
          "WHERE n1.n_regionkey = r.r_regionkey AND r.r_name = 'Europe') nr " +
          "ON nr.n_nationkey = UNICODE(c.c_state)) cnr " +
          "ON cnr.c_id = o.o_c_id AND cnr.c_w_id = o.o_w_id AND cnr.c_d_id = o.o_d_id " +
          "AND i.i_data LIKE '%b' AND i.i_id = ol.ol_i_id " +
          "AND ol.ol_i_id < 1000 " +
          "AND o.o_entry_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000' " +
          ") rn1cooli " +//
          "ON rn1cooli.ol_i_id = s.s_i_id " +
          "AND rn1cooli.ol_supply_w_id = s.s_w_id) rn1coolis JOIN " +
          "(SELECT su.su_suppkey, n2.n_name " +
          "FROM supplier su, nation n2 " +
          "WHERE su.su_nationkey = n2.n_nationkey) sun2 " +
          "ON MOD(rn1coolis.s_w_id * rn1coolis.s_i_id, 10000) = sun2.su_suppkey " +
          "GROUP BY DATE_PART(year, rn1coolis.o_entry_d) " +
          "ORDER BY l_year",
      // Q09
      "SELECT sun.n_name, DATE_PART(year, oolis.o_entry_d) as l_year, round (SUM(oolis.ol_amount), 2) as SUM_profit " +
          "FROM " +
          "(SELECT s.s_w_id, s.s_i_id, ooli.o_entry_d, ooli.ol_amount " +
          "FROM stock s JOIN " +
          "(SELECT ol.ol_i_id, ol.ol_supply_w_id, ol.ol_amount, o.o_entry_d " +
          "FROM orders o,  order_line ol, item i " +
          "WHERE o.o_id = ol.ol_o_id " +
          "AND  i.i_data LIKE '%bb' and ol.ol_i_id = i.i_id) ooli " +
          "ON ooli.ol_i_id = s.s_i_id and ooli.ol_supply_w_id = s.s_w_id) oolis JOIN " +
          "(SELECT su.su_suppkey, n.n_name " +
          "FROM supplier su, nation n " +
          "WHERE su.su_nationkey = n.n_nationkey) sun " +
          "ON MOD(oolis.s_w_id * oolis.s_i_id, 10000) = sun.su_suppkey " +
          "GROUP BY sun.n_name, DATE_PART(year, oolis.o_entry_d) " +
          "ORDER BY sun.n_name, l_year DESC",
      // Q10
      "SELECT c.c_id, c.c_last, SUM(ol.ol_amount) as revenue, c.c_city, c.c_phone, n.n_name " +
          "FROM nation n, customer c, orders o, order_line ol " +
          "WHERE o.o_id = ol.ol_o_id " +
          "AND  c.c_id = o.o_c_id " +
          "AND  c.c_w_id = o.o_w_id " +
          "AND  c.c_d_id = o.o_d_id " +
          "AND  o.o_entry_d >= '2015-10-01 00:00:00.000000' " +
          "AND o.o_entry_d < '2016-01-01 00:00:00.000000' " +
          "AND  n.n_nationkey = UNICODE(c.c_state) " +
          "GROUP BY c.c_id, c.c_last, c.c_city, c.c_phone, n.n_name " +
          "ORDER BY revenue DESC " +
          "LIMIT 20",
      // Q11
      "SELECT s.s_i_id, SUM(s.s_order_cnt) as ordercount " +
          "FROM   nation n, supplier su, stock s " +
          "WHERE  MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey " +
          "AND  su.su_nationkey = n.n_nationkey " +
          "AND  n.n_name = 'Germany' " +
          "GROUP BY s.s_i_id " +
          "HAVING SUM(s.s_order_cnt) > " +
          "(SELECT VALUE SUM(s1.s_order_cnt) * 0.00005 " +
          "FROM nation n1, supplier su1, stock s1 " +
          "WHERE MOD(s1.s_w_id * s1.s_i_id, 10000) = su1.su_suppkey " +
          "AND su1.su_nationkey = n1.n_nationkey " +
          "AND n1.n_name = 'Germany')[0] " +
          "ORDER BY ordercount DESC",
      // Q12
      "SELECT o.o_ol_cnt, " +
          "SUM (case when o.o_carrier_id = 1 or o.o_carrier_id = 2 " +
          "THEN 1 ELSE 0 END) AS high_line_COUNT, " +
          "SUM (case when o.o_carrier_id <> 1 AND o.o_carrier_id <> 2 " +
          "THEN 1 ELSE 0 END) AS low_line_COUNT " +
          "FROM orders o, order_line ol " +
          "WHERE o.o_id = ol.ol_o_id " +
          "AND  o.o_entry_d <= ol.ol_delivery_d " +
          "AND  ol.ol_delivery_d >= '2016-01-01 00:00:00.000000' AND  ol.ol_delivery_d < '2017-01-01 00:00:00.000000' " +
          "GROUP BY o.o_ol_cnt " +
          "ORDER BY o.o_ol_cnt",
      // Q13
      "SELECT c_orders.c_count, COUNT(*) as custdist " +
          "FROM  (SELECT c.c_id, COUNT(o.o_id) as c_count " +
          "FROM customer c LEFT OUTER JOIN orders o ON ( " +
          "c.c_w_id = o.o_w_id " +
          "AND c.c_d_id = o.o_d_id " +
          "AND c.c_id = o.o_c_id " +
          "AND o.o_carrier_id > 8) " +
          "GROUP BY c.c_id) as c_orders " +
          "GROUP BY c_orders.c_count " +
          "ORDER BY custdist DESC, c_orders.c_count DESC",
      // Q14
      "SELECT 100.00 * SUM(CASE WHEN i.i_data LIKE 'pr%' " +
          "THEN ol.ol_amount ELSE 0 END) / " +
          "(1+SUM(ol.ol_amount)) AS promo_revenue " +
          "FROM item i, order_line ol " +
          "WHERE ol.ol_i_id = i.i_id " +
          "AND ol.ol_delivery_d >= '2017-09-01 00:00:00.000000' AND ol.ol_delivery_d < '2017-10-01 00:00:00.000000'",
      // Q15
      "WITH revenue AS ( " +
          "SELECT MOD(s.s_w_id * s.s_i_id, 10000) as supplier_no, SUM(ol.ol_amount) AS total_revenue " +
          "FROM   stock s, order_line ol " +
          "WHERE ol.ol_i_id = s.s_i_id " +
          "AND ol.ol_supply_w_id = s.s_w_id " +
          "AND ol.ol_delivery_d >= '2018-01-01 00:00:00.000000' AND ol.ol_delivery_d < '2018-04-01 00:00:00.000000' " +
          "GROUP BY MOD(s.s_w_id * s.s_i_id, 10000)) " +
          "SELECT su.su_suppkey, su.su_name, su.su_address, su.su_phone, r.total_revenue " +
          "FROM revenue r,  supplier su " +
          "WHERE  su.su_suppkey = r.supplier_no " +
          "AND  r.total_revenue = (SELECT VALUE max(r1.total_revenue) FROM revenue r1)[0] " +
          "ORDER BY su.su_suppkey",
      // Q16
      "SELECT i.i_name, SUBSTR1(i.i_data, 1, 3) AS brand, i.i_price, " +
          "COUNT(DISTINCT (MOD(s.s_w_id * s.s_i_id, 10000))) AS supplier_cnt " +
          "FROM stock s, item i " +
          "WHERE i.i_id = s.s_i_id " +
          "AND i.i_data not LIKE 'zz%' " +
          "AND (MOD(s.s_w_id * s.s_i_id, 10000) NOT IN " +
          "(SELECT VALUE su.su_suppkey " +
          "FROM supplier su " +
          "WHERE su.su_comment LIKE '%Customer%Complaints%')) " +
          "GROUP BY i.i_name, SUBSTR1(i.i_data, 1, 3), i.i_price " +
          "ORDER BY supplier_cnt DESC",
      // Q17
      "SELECT SUM(ol.ol_amount) / 2.0 AS AVG_yearly " +
          "FROM  (SELECT i.i_id, AVG(ol1.ol_quantity) AS a " +
          "FROM   item i, order_line ol1 " +
          "WHERE  i.i_data LIKE '%b' " +
          "AND  ol1.ol_i_id = i.i_id " +
          "GROUP BY i.i_id) t, order_line ol " +
          "WHERE ol.ol_i_id = t.i_id " +
          "AND ol.ol_quantity <= t.a",
      // Q18
      "SELECT c.c_last, c.c_id o_id, o.o_entry_d, o.o_ol_cnt, SUM(ol.ol_amount) " +
          "FROM orders o, order_line ol, customer c " +
          "WHERE o.o_id = ol.ol_o_id " +
          "AND  c.c_id = o.o_c_id AND  c.c_w_id = o.o_w_id AND  c.c_d_id = o.o_d_id " +
          "GROUP BY o.o_id, o.o_w_id, o.o_d_id, c.c_id, c.c_last, o.o_entry_d, o.o_ol_cnt " +
          "HAVING SUM(ol.ol_amount) > 200 " +
          "ORDER BY SUM(ol.ol_amount) DESC, o.o_entry_d " +
          "LIMIT 100",
      // Q19
      "SELECT SUM(ol.ol_amount) AS revenue " +
          "FROM orders o, order_line ol, item i " +
          "WHERE o.o_id = ol.ol_o_id " +
          "AND  (( " +
          "i.i_data LIKE '%h' " +
          "AND ol.ol_quantity >= 7 AND ol.ol_quantity <= 17 " +
          "AND i.i_price between 1 AND 5 " +
          "AND o.o_w_id IN [1, 29, 70] " +
          ") OR ( " +
          "i.i_data LIKE '%t' " +
          "AND ol.ol_quantity >= 16 AND ol.ol_quantity <= 26 " +
          "AND i.i_price between 1 AND 10 " +
          "AND o.o_w_id IN [1, 17, 6] " +
          ") OR ( " +
          "i.i_data LIKE '%m' " +
          "AND ol.ol_quantity >= 24 AND ol.ol_quantity <= 34 " +
          "AND i.i_price between 1 AND 15 " +
          "AND  o.o_w_id IN [1, 95, 15] " +
          ")) " +
          "AND ol.ol_i_id = i.i_id " +
          "AND i.i_price between 1 AND 15",
      // Q20
      "SELECT su.su_name, su.su_address " +
          "FROM   supplier su, nation n " +
          "WHERE  su.su_suppkey IN " +
          "(SELECT VALUE MOD(s.s_i_id * s.s_w_id, 10000) " +
          "FROM   stock s, order_line ol " +
          "WHERE  s.s_i_id IN " +
          "(SELECT VALUE i.i_id " +
          "FROM item i " +
          "WHERE i.i_data LIKE 'co%') " +
          "AND ol.ol_i_id=s.s_i_id " +
          "AND ol.ol_delivery_d >= '2016-01-01 12:00:00' " +
          "AND ol.ol_delivery_d < '2017-01-01 12:00:00' " +
          "GROUP BY s.s_i_id, s.s_w_id, s.s_quantity " +
          "HAVING 20*s.s_quantity > SUM(ol.ol_quantity)) " +
          "AND su.su_nationkey = n.n_nationkey " +
          "AND n.n_name = 'Germany'  " +
          "ORDER BY su.su_name",
      // Q21
      "SELECT z.su_name, count (*) AS numwait " +
          "FROM (SELECT x.su_name " +
          "FROM (SELECT o1.o_id, o1.o_w_id, o1.o_d_id, ol1.ol_delivery_d,  " +
          "n.n_nationkey, su.su_suppkey, s.s_w_id, s.s_i_id, su.su_name " +
          "FROM nation n, supplier su, stock s, orders o1, order_line ol1 " +
          "WHERE o1.o_id = ol1.ol_o_id " +
          "AND  o1.o_w_id = s.s_w_id " +
          "AND ol1.ol_i_id = s.s_i_id " +
          "AND MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey " +
          "AND ol1.ol_delivery_d > date_add_str(o1.o_entry_d, 150, 'day') " +
          "AND o1.o_entry_d between '2017-12-01 00:00:00' and '2017-12-31 00:00:00' " +
          "AND su.su_nationkey = n.n_nationkey " +
          "AND n.n_name = 'Peru') x " +
          "LEFT OUTER JOIN " +
          "(SELECT o2.o_id, o2.o_w_id, o2.o_d_id, ol2.ol_delivery_d " +
          "FROM orders o2, order_line ol2 " +
          "WHERE o2.o_id = ol2.ol_o_id " +
          "AND o2.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00') y " +
          "ON y.o_id = x.o_id AND y.o_w_id = x.o_w_id AND y.o_d_id = x.o_d_id " +
          "AND y.ol_delivery_d > x.ol_delivery_d " +
          "GROUP BY x.o_w_id, x.o_d_id, x.o_id, x.n_nationkey, x.su_suppkey, x.s_w_id, x.s_i_id, x.su_name " +
          "HAVING COUNT (y.o_id) = 0) z " +
          "GROUP BY z.su_name " +
          "LIMIT 100",
      // Q22
      "SELECT SUBSTR1(c.c_state,1,1) AS country, COUNT(*) AS numcust, SUM(c.c_balance) AS totacctbal " +
          "FROM customer c " +
          "WHERE SUBSTR1(c.c_phone,1,1) IN ['1','2','3','4','5','6','7'] " +
          "AND c.c_balance > (SELECT VALUE AVG(c1.c_balance) " +
          "FROM customer c1 " +
          "WHERE c1.c_balance > 0.00 " +
          "AND SUBSTR1(c1.c_phone,1,1) IN ['1','2','3','4','5','6','7'])[0] " +
          "AND NOT EXISTS (SELECT VALUE 1 " +
          "FROM orders o " +
          "WHERE o.o_c_id = c.c_id AND o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id " +
          "AND o.o_entry_d BETWEEN '2013-12-01 00:00:00' AND '2013-12-31 00:00:00') " +
          "GROUP BY SUBSTR1(c.c_state,1,1) " +
          "ORDER BY SUBSTR1(c.c_state,1,1)"
  };

  public final int[][] queryPermutations = {
      {  14,  2,  9,  20,  6,  17,  18,  8,  21,  13,  3,  22,  16,  4,  11,  15,  1,  10,  19,  5,  7,  12,  },
      {  21,  3,  18,  5,  11,  7,  6,  20,  17,  12,  16,  15,  13,  10,  2,  8,  14,  19,  9,  22,  1,  4,  },
      {  6,  17,  14,  16,  19,  10,  9,  2,  15,  8,  5,  22,  12,  7,  13,  18,  1,  4,  20,  3,  11,  21,  },
      {  8,  5,  4,  6,  17,  7,  1,  18,  22,  14,  9,  10,  15,  11,  20,  2,  21,  19,  13,  16,  12,  3,  },
      {  5,  21,  14,  19,  15,  17,  12,  6,  4,  9,  8,  16,  11,  2,  10,  18,  1,  13,  7,  22,  3,  20,  },
      {  21,  15,  4,  6,  7,  16,  19,  18,  14,  22,  11,  13,  3,  1,  2,  5,  8,  20,  12,  17,  10,  9,  },
      {  10,  3,  15,  13,  6,  8,  9,  7,  4,  11,  22,  18,  12,  1,  5,  16,  2,  14,  19,  20,  17,  21,  },
      {  18,  8,  20,  21,  2,  4,  22,  17,  1,  11,  9,  19,  3,  13,  5,  7,  10,  16,  6,  14,  15,  12,  },
      {  19,  1,  15,  17,  5,  8,  9,  12,  14,  7,  4,  3,  20,  16,  6,  22,  10,  13,  2,  21,  18,  11,  },
      {  8,  13,  2,  20,  17,  3,  6,  21,  18,  11,  19,  10,  15,  4,  22,  1,  7,  12,  9,  14,  5,  16,  },
      {  6,  15,  18,  17,  12,  1,  7,  2,  22,  13,  21,  10,  14,  9,  3,  16,  20,  19,  11,  4,  8,  5,  },
      {  15,  14,  18,  17,  10,  20,  16,  11,  1,  8,  4,  22,  5,  12,  3,  9,  21,  2,  13,  6,  19,  7,  },
      {  1,  7,  16,  17,  18,  22,  12,  6,  8,  9,  11,  4,  2,  5,  20,  21,  13,  10,  19,  3,  14,  15,  },
      {  21,  17,  7,  3,  1,  10,  12,  22,  9,  16,  6,  11,  2,  4,  5,  14,  8,  20,  13,  18,  15,  19,  },
      {  2,  9,  5,  4,  18,  1,  20,  15,  16,  17,  7,  21,  13,  14,  19,  8,  22,  11,  10,  3,  12,  6,  },
      {  16,  9,  17,  8,  14,  11,  10,  12,  6,  21,  7,  3,  15,  5,  22,  20,  1,  13,  19,  2,  4,  18,  },
      {  1,  3,  6,  5,  2,  16,  14,  22,  17,  20,  4,  9,  10,  11,  15,  8,  12,  19,  18,  13,  7,  21,  },
      {  3,  16,  5,  11,  21,  9,  2,  15,  10,  18,  17,  7,  8,  19,  14,  13,  1,  4,  22,  20,  6,  12,  },
      {  14,  4,  13,  5,  21,  11,  8,  6,  3,  17,  2,  20,  1,  19,  10,  9,  12,  18,  15,  7,  22,  16,  },
      {  4,  12,  22,  14,  5,  15,  16,  2,  8,  10,  17,  9,  21,  7,  3,  6,  13,  18,  11,  20,  19,  1,  },
      {  16,  15,  14,  13,  4,  22,  18,  19,  7,  1,  12,  17,  5,  10,  20,  3,  9,  21,  11,  2,  6,  8,  },
      {  20,  14,  21,  12,  15,  17,  4,  19,  13,  10,  11,  1,  16,  5,  18,  7,  8,  22,  9,  6,  3,  2,  },
      {  16,  14,  13,  2,  21,  10,  11,  4,  1,  22,  18,  12,  19,  5,  7,  8,  6,  3,  15,  20,  9,  17,  },
      {  18,  15,  9,  14,  12,  2,  8,  11,  22,  21,  16,  1,  6,  17,  5,  10,  19,  4,  20,  13,  3,  7,  },
      {  7,  3,  10,  14,  13,  21,  18,  6,  20,  4,  9,  8,  22,  15,  2,  1,  5,  12,  19,  17,  11,  16,  },
      {  18,  1,  13,  7,  16,  10,  14,  2,  19,  5,  21,  11,  22,  15,  8,  17,  20,  3,  4,  12,  6,  9,  },
      {  13,  2,  22,  5,  11,  21,  20,  14,  7,  10,  4,  9,  19,  18,  6,  3,  1,  8,  15,  12,  17,  16,  },
      {  14,  17,  21,  8,  2,  9,  6,  4,  5,  13,  22,  7,  15,  3,  1,  18,  16,  11,  10,  12,  20,  19,  },
      {  10,  22,  1,  12,  13,  18,  21,  20,  2,  14,  16,  7,  15,  3,  4,  17,  5,  19,  6,  8,  9,  11,  },
      {  10,  8,  9,  18,  12,  6,  1,  5,  20,  11,  17,  22,  16,  3,  13,  2,  15,  21,  14,  19,  7,  4,  },
      {  7,  17,  22,  5,  3,  10,  13,  18,  9,  1,  14,  15,  21,  19,  16,  12,  8,  6,  11,  20,  4,  2,  },
      {  2,  9,  21,  3,  4,  7,  1,  11,  16,  5,  20,  19,  18,  8,  17,  13,  10,  12,  15,  6,  14,  22,  },
      {  15,  12,  8,  4,  22,  13,  16,  17,  18,  3,  7,  5,  6,  1,  9,  11,  21,  10,  14,  20,  19,  2,  },
      {  15,  16,  2,  11,  17,  7,  5,  14,  20,  4,  21,  3,  10,  9,  12,  8,  13,  6,  18,  19,  22,  1,  },
      {  1,  13,  11,  3,  4,  21,  6,  14,  15,  22,  18,  9,  7,  5,  10,  20,  12,  16,  17,  8,  19,  2,  },
      {  14,  17,  22,  20,  8,  16,  5,  10,  1,  13,  2,  21,  12,  9,  4,  18,  3,  7,  6,  19,  15,  11,  },
      {  9,  17,  7,  4,  5,  13,  21,  18,  11,  3,  22,  1,  6,  16,  20,  14,  15,  10,  8,  2,  12,  19,  },
      {  13,  14,  5,  22,  19,  11,  9,  6,  18,  15,  8,  10,  7,  4,  17,  16,  3,  1,  12,  2,  21,  20,  },
      {  20,  5,  4,  14,  11,  1,  6,  16,  8,  22,  7,  3,  2,  12,  21,  19,  17,  13,  10,  15,  18,  9,  },
      {  3,  7,  14,  15,  6,  5,  21,  20,  18,  10,  4,  16,  19,  1,  13,  9,  8,  17,  11,  12,  22,  2,  },
      {  13,  15,  17,  1,  22,  11,  3,  4,  7,  20,  14,  21,  9,  8,  2,  18,  16,  6,  10,  12,  5,  19,  },
  };

  @Override
  public String[] getQueryList() {
    return sqlStatements;
  }

  @Override
  public int[][] getQueryPermutations() {
    return queryPermutations;
  }
}
