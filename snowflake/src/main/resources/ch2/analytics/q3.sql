WITH co as (SELECT o.o_id, o.o_w_id, o.o_d_id, o.o_entry_d, ol.ol_amount
            FROM orders o,
                 customer c,
                 order_line ol
            WHERE o.o_id = ol.ol_o_id
              AND c.c_state LIKE 'A%'
              AND c.c_id = o.o_c_id
              AND c.c_w_id = o.o_w_id
              AND c.c_d_id = o.o_d_id
              AND o.o_entry_d < '2017-03-15 00:00:00.000000')
SELECT co.o_id, co.o_w_id, co.o_d_id, SUM(co.ol_amount) as revenue, co.o_entry_d
FROM co,
     new_orders no
WHERE no.no_w_id = co.o_w_id AND no.no_d_id = co.o_d_id AND no.no_o_id = co.o_id
GROUP BY co.o_id, co.o_w_id, co.o_d_id, co.o_entry_d
ORDER BY revenue DESC, co.o_entry_d
