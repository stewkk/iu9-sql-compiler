SELECT SUM(lo.lo_revenue) AS lo_revenue, d.d_year, p.p_brand
FROM lineorder AS lo
JOIN date AS d ON lo.lo_orderdate = d.d_datekey
JOIN part AS p ON lo.lo_partkey = p.p_partkey
JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey
WHERE p.p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
  AND s.s_region = 'ASIA'
GROUP BY d.d_year, p.p_brand
ORDER BY d.d_year, p.p_brand;
