SELECT SUM(ol.ol_amount) AS revenue
FROM orders o,
     order_line ol,
     item i
WHERE o.o_id = ol.ol_o_id
  AND ((i.i_data LIKE '%h' AND ol.ol_quantity >= 7 AND ol.ol_quantity <= 17 AND i.i_price between 1 AND 5 AND
        ARRAY_CONTAINS(o.o_w_id, ARRAY_CONSTRUCT(1, 29, 70))) OR
       (i.i_data LIKE '%t' AND ol.ol_quantity >= 16 AND ol.ol_quantity <= 26 AND i.i_price between 1 AND 10 AND
        ARRAY_CONTAINS(o.o_w_id, ARRAY_CONSTRUCT(1, 17, 6))) OR
       (i.i_data LIKE '%m' AND ol.ol_quantity >= 24 AND ol.ol_quantity <= 34 AND i.i_price between 1 AND 15 AND
        ARRAY_CONTAINS(o.o_w_id, ARRAY_CONSTRUCT(1, 95, 15))))
  AND ol.ol_i_id = i.i_id
  AND i.i_price between 1 AND 15
