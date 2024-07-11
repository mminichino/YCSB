SELECT SUBSTR(c.c_state, 1, 1) AS country, COUNT(*) AS numcust, SUM(c.c_balance) AS totacctbal
FROM customer c
WHERE ARRAY_CONTAINS(SUBSTR(c.c_phone, 1, 1)::VARIANT, ARRAY_CONSTRUCT('1', '2', '3', '4', '5', '6', '7'))
  AND c.c_balance > (SELECT AVG(c1.c_balance)
                     FROM customer c1
                     WHERE c1.c_balance > 0.00
                       AND ARRAY_CONTAINS(SUBSTR(c1.c_phone, 1, 1)::VARIANT,
                                          ARRAY_CONSTRUCT('1', '2', '3', '4', '5', '6', '7')))
  AND NOT EXISTS (SELECT 1
                  FROM orders o
                  WHERE o.o_c_id = c.c_id
                    AND o.o_w_id = c.c_w_id
                    AND o.o_d_id = c.c_d_id
                    AND o.o_entry_d BETWEEN '2013-12-01 00:00:00' AND '2013-12-31 00:00:00')
GROUP BY SUBSTR(c.c_state, 1, 1)
ORDER BY SUBSTR(c.c_state, 1, 1)
