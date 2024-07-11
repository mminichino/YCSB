SELECT s.s_i_id, SUM(s.s_order_cnt) as ordercount
FROM nation n,
     supplier su,
     stock s
WHERE MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_name = 'Germany'
GROUP BY s.s_i_id
HAVING SUM(s.s_order_cnt) > (SELECT SUM(s1.s_order_cnt) * 0.00005
                             FROM nation n1,
                                  supplier su1,
                                  stock s1
                             WHERE MOD(s1.s_w_id * s1.s_i_id, 10000) = su1.su_suppkey
                               AND su1.su_nationkey = n1.n_nationkey
                               AND n1.n_name = 'Germany')
ORDER BY ordercount DESC
