SELECT o.o_ol_cnt, COUNT(*) as order_COUNT
FROM orders o,
     order_line ol
WHERE o.o_id = ol.ol_o_id
  AND o.o_entry_d >= '2015-07-01 00:00:00.000000'
  AND o.o_entry_d < '2015-10-01 00:00:00.000000'
  AND ol.ol_delivery_d >= dateadd(week, 1, o.o_entry_d)
GROUP BY o.o_ol_cnt
ORDER BY o.o_ol_cnt
