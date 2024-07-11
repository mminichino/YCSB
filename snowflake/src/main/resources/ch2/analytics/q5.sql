SELECT cnros.n_name, ROUND(sum(cnros.ol_amount), 2) as revenue
FROM (SELECT cnro.ol_amount, cnro.n_name, cnro.n_nationkey, s.s_w_id, s.s_i_id
      FROM stock s
               JOIN (SELECT o.o_w_id, ol.ol_amount, ol.ol_i_id, cnr.n_name, cnr.n_nationkey
                     FROM orders o
                              JOIN order_line ol ON o.o_id = ol.ol_o_id
                              JOIN (SELECT c.c_id, c.c_w_id, c.c_d_id, nr.n_name, nr.n_nationkey
                                    FROM customer c
                                             JOIN (SELECT n.n_nationkey, n.n_name
                                                   FROM nation n,
                                                        region r
                                                   WHERE n.n_regionkey = r.r_regionkey
                                                     AND r.r_name = 'Asia') nr
                                                  ON UNICODE(c.c_state) = nr.n_nationkey) cnr
                                   ON o.o_entry_d >= '2016-01-01 00:00:00.000000' AND
                                      o.o_entry_d < '2017-01-01 00:00:00.000000' AND cnr.c_id = o.o_c_id AND
                                      cnr.c_w_id = o.o_w_id AND cnr.c_d_id = o.o_d_id) cnro
                    ON cnro.o_w_id = s.s_w_id AND cnro.ol_i_id = s.s_i_id) cnros
         JOIN supplier su
              ON MOD(cnros.s_w_id * cnros.s_i_id, 10000) = su.su_suppkey AND su.su_nationkey = cnros.n_nationkey
GROUP BY cnros.n_name
ORDER BY revenue DESC
