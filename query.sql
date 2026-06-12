SELECT d.d_year, c.c_nation, SUM(lo.lo_revenue - lo.lo_supplycost) AS profit
FROM lineorder AS lo
JOIN date AS d ON lo.lo_orderdate = d.d_datekey
JOIN customer AS c ON lo.lo_custkey = c.c_custkey
JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey
JOIN part AS p ON lo.lo_partkey = p.p_partkey
WHERE c.c_region = 'AMERICA'
  AND s.s_region = 'AMERICA'
  AND p.p_mfgr IN ('MFGR#1', 'MFGR#2')
GROUP BY d.d_year, c.c_nation
ORDER BY d.d_year, c.c_nation;
