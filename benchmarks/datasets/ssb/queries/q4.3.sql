SELECT d.d_year, s.s_city, p.p_brand, SUM(lo.lo_revenue - lo.lo_supplycost) AS profit
FROM lineorder AS lo
JOIN date AS d ON lo.lo_orderdate = d.d_datekey
JOIN customer AS c ON lo.lo_custkey = c.c_custkey
JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey
JOIN part AS p ON lo.lo_partkey = p.p_partkey
WHERE s.s_nation = 'UNITED STATES'
  AND d.d_year IN (1997, 1998)
  AND p.p_category = 'MFGR#14'
GROUP BY d.d_year, s.s_city, p.p_brand
ORDER BY d.d_year, s.s_city, p.p_brand;
