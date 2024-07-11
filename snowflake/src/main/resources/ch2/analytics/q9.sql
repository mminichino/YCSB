SELECT sun.n_name, DATE_PART(year, oolis.o_entry_d) as l_year, round(SUM(oolis.ol_amount), 2) as SUM_profit
FROM (SELECT s.s_w_id, s.s_i_id, ooli.o_entry_d, ooli.ol_amount
      FROM stock s
               JOIN (SELECT ol.ol_i_id, ol.ol_supply_w_id, ol.ol_amount, o.o_entry_d
                     FROM orders o,
                          order_line ol,
                          item i
                     WHERE o.o_id = ol.ol_o_id
                       AND i.i_data LIKE '%bb'
                       and ol.ol_i_id = i.i_id) ooli
                    ON ooli.ol_i_id = s.s_i_id and ooli.ol_supply_w_id = s.s_w_id) oolis
         JOIN (SELECT su.su_suppkey, n.n_name
               FROM supplier su,
                    nation n
               WHERE su.su_nationkey = n.n_nationkey) sun ON MOD(oolis.s_w_id * oolis.s_i_id, 10000) = sun.su_suppkey
GROUP BY sun.n_name, DATE_PART(year, oolis.o_entry_d)
ORDER BY sun.n_name, l_year DESC
