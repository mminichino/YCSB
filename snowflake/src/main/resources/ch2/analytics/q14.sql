SELECT
    100.00 * SUM(CASE WHEN i.i_data LIKE 'pr%' THEN ol.ol_amount ELSE 0 END) / (1 + SUM(ol.ol_amount)) AS promo_revenue
FROM item i,
     order_line ol
WHERE ol.ol_i_id = i.i_id
  AND ol.ol_delivery_d >= '2017-09-01 00:00:00.000000'
  AND ol.ol_delivery_d < '2017-10-01 00:00:00.000000'
