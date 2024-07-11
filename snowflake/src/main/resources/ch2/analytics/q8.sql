SELECT DATE_PART(year, rn1coolis.o_entry_d) as l_year,
       ROUND((SUM(case when sun2.n_name = 'Germany' then rn1coolis.ol_amount else 0 end) / SUM(rn1coolis.ol_amount)),
             2)                             as mkt_share
FROM (SELECT rn1cooli.o_entry_d, rn1cooli.ol_amount, s.s_w_id, s.s_i_id
      FROM stock s
               JOIN (SELECT o.o_entry_d, ol.ol_i_id, ol.ol_amount, ol.ol_supply_w_id
                     FROM orders o
                              JOIN order_line ol ON o.o_id = ol.ol_o_id
                              JOIN item i
                              JOIN (SELECT c.c_id, c.c_w_id, c.c_d_id
                                    FROM customer c
                                             JOIN (SELECT n1.n_nationkey
                                                   FROM nation n1,
                                                        region r
                                                   WHERE n1.n_regionkey = r.r_regionkey
                                                     AND r.r_name = 'Europe') nr
                                                  ON nr.n_nationkey = UNICODE(c.c_state)) cnr
                                   ON cnr.c_id = o.o_c_id AND cnr.c_w_id = o.o_w_id AND cnr.c_d_id = o.o_d_id AND
                                      i.i_data LIKE '%b' AND i.i_id = ol.ol_i_id AND ol.ol_i_id < 1000 AND
                                      o.o_entry_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000') rn1cooli
                    ON rn1cooli.ol_i_id = s.s_i_id AND rn1cooli.ol_supply_w_id = s.s_w_id) rn1coolis
         JOIN (SELECT su.su_suppkey, n2.n_name
               FROM supplier su,
                    nation n2
               WHERE su.su_nationkey = n2.n_nationkey) sun2
              ON MOD(rn1coolis.s_w_id * rn1coolis.s_i_id, 10000) = sun2.su_suppkey
GROUP BY DATE_PART(year, rn1coolis.o_entry_d)
ORDER BY l_year
