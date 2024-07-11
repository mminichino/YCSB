SELECT su.su_suppkey,
       su.su_name,
       n.n_name,
       i.i_id,
       i.i_name,
       su.su_address,
       su.su_phone,
       su.su_comment
FROM (SELECT s1.s_i_id as m_i_id, MIN(s1.s_quantity) as m_s_quantity
      FROM stock s1,
           (SELECT su1.su_suppkey
            FROM supplier su1,
                 (SELECT n1.n_nationkey
                  from nation n1,
                       region r1
                  WHERE n1.n_regionkey = r1.r_regionkey
                    AND r1.r_name LIKE 'Europ%') t1
            WHERE su1.su_nationkey = t1.n_nationkey) t2
      WHERE MOD(s1.s_w_id * s1.s_i_id, 10000) = t2.su_suppkey
      GROUP BY s1.s_i_id) m,
     item i,
     stock s,
     supplier su,
     nation n,
     region r
WHERE i.i_id = s.s_i_id
  AND MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND i.i_data LIKE '%b'
  AND r.r_name LIKE 'Europ%'
  AND i.i_id = m.m_i_id
  AND s.s_quantity = m.m_s_quantity
ORDER BY n.n_name, su.su_name, i.i_id
limit 100
