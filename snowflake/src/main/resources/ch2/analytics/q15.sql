WITH revenue AS (SELECT MOD(s.s_w_id * s.s_i_id, 10000) as supplier_no, SUM(ol.ol_amount) AS total_revenue
                 FROM stock s,
                      order_line ol
                 WHERE ol.ol_i_id = s.s_i_id
                   AND ol.ol_supply_w_id = s.s_w_id
                   AND ol.ol_delivery_d >= '2018-01-01 00:00:00.000000'
                   AND ol.ol_delivery_d < '2018-04-01 00:00:00.000000'
                 GROUP BY MOD(s.s_w_id * s.s_i_id, 10000))
SELECT su.su_suppkey, su.su_name, su.su_address, su.su_phone, r.total_revenue
FROM revenue r,
     supplier su
WHERE su.su_suppkey = r.supplier_no
  AND r.total_revenue = (SELECT max(r1.total_revenue) FROM revenue r1)
ORDER BY su.su_suppkey
