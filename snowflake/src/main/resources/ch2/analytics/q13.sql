SELECT c_orders.c_count, COUNT(*) as custdist
FROM (SELECT c.c_id, COUNT(o.o_id) as c_count
      FROM customer c
               LEFT OUTER JOIN orders o ON (c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id AND
                                            o.o_carrier_id > 8)
      GROUP BY c.c_id) as c_orders
GROUP BY c_orders.c_count
ORDER BY custdist DESC, c_orders.c_count DESC
