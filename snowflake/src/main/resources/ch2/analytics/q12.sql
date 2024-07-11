SELECT o.o_ol_cnt,
       SUM(case when o.o_carrier_id = 1 or o.o_carrier_id = 2 THEN 1 ELSE 0 END)    AS high_line_COUNT,
       SUM(case when o.o_carrier_id <> 1 AND o.o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_COUNT
FROM orders o,
     order_line ol
WHERE o.o_id = ol.ol_o_id
  AND o.o_entry_d <= ol.ol_delivery_d
  AND ol.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND ol.ol_delivery_d < '2017-01-01 00:00:00.000000'
GROUP BY o.o_ol_cnt
ORDER BY o.o_ol_cnt
