SELECT ol.ol_number,
       SUM(ol.ol_quantity) as sum_qty,
       SUM(ol.ol_amount)   as sum_amount,
       AVG(ol.ol_quantity) as avg_qty,
       AVG(ol.ol_amount)   as avg_amount,
       COUNT(*)            as COUNT_order
FROM order_line ol
WHERE ol.ol_delivery_d > '2014-07-01 00:00:00'
GROUP BY ol.ol_number
ORDER BY ol.ol_number
