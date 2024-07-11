SELECT su.su_name, su.su_address
FROM supplier su,
     nation n
WHERE su.su_suppkey IN (SELECT MOD(s.s_i_id * s.s_w_id, 10000)
                        FROM stock s,
                             order_line ol
                        WHERE s.s_i_id IN (SELECT i.i_id FROM item i WHERE i.i_data LIKE 'co%')
                          AND ol.ol_i_id = s.s_i_id
                          AND ol.ol_delivery_d >= '2016-01-01 12:00:00'
                          AND ol.ol_delivery_d < '2017-01-01 12:00:00'
                        GROUP BY s.s_i_id, s.s_w_id, s.s_quantity
                        HAVING 20 * s.s_quantity > SUM(ol.ol_quantity))
  AND su.su_nationkey = n.n_nationkey
  AND n.n_name = 'Germany'
ORDER BY su.su_name
