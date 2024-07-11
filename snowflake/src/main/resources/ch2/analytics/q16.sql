SELECT i.i_name,
       SUBSTR(i.i_data, 1, 3)                            AS brand,
       i.i_price,
       COUNT(DISTINCT (MOD(s.s_w_id * s.s_i_id, 10000))) AS supplier_cnt
FROM stock s,
     item i
WHERE i.i_id = s.s_i_id
  AND i.i_data not LIKE 'zz%'
  AND (MOD(s.s_w_id * s.s_i_id, 10000) NOT IN
       (SELECT su.su_suppkey FROM supplier su WHERE su.su_comment LIKE '%Customer%Complaints%'))
GROUP BY i.i_name, SUBSTR(i.i_data, 1, 3), i.i_price
ORDER BY supplier_cnt DESC
