SELECT z.su_name, count(*) AS numwait
FROM (SELECT x.su_name
      FROM (SELECT o1.o_id,
                   o1.o_w_id,
                   o1.o_d_id,
                   ol1.ol_delivery_d,
                   n.n_nationkey,
                   su.su_suppkey,
                   s.s_w_id,
                   s.s_i_id,
                   su.su_name
            FROM nation n,
                 supplier su,
                 stock s,
                 orders o1,
                 order_line ol1
            WHERE o1.o_id = ol1.ol_o_id
              AND o1.o_w_id = s.s_w_id
              AND ol1.ol_i_id = s.s_i_id
              AND MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
              AND ol1.ol_delivery_d > dateadd(day, 150, o1.o_entry_d)
              AND o1.o_entry_d between '2017-12-01 00:00:00' and '2017-12-31 00:00:00'
              AND su.su_nationkey = n.n_nationkey
              AND n.n_name = 'Peru') x
               LEFT OUTER JOIN (SELECT o2.o_id, o2.o_w_id, o2.o_d_id, ol2.ol_delivery_d
                                FROM orders o2,
                                     order_line ol2
                                WHERE o2.o_id = ol2.ol_o_id
                                  AND o2.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00') y
                               ON y.o_id = x.o_id AND y.o_w_id = x.o_w_id AND y.o_d_id = x.o_d_id AND
                                  y.ol_delivery_d > x.ol_delivery_d
      GROUP BY x.o_w_id, x.o_d_id, x.o_id, x.n_nationkey, x.su_suppkey, x.s_w_id, x.s_i_id, x.su_name
      HAVING COUNT(y.o_id) = 0) z
GROUP BY z.su_name LIMIT 100
