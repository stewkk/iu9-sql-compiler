SELECT c.c_city, s.s_city, d.d_year, SUM(lo.lo_revenue) AS lo_revenue
FROM lineorder AS lo
JOIN customer AS c ON lo.lo_custkey = c.c_custkey
JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey
JOIN date AS d ON lo.lo_orderdate = d.d_datekey
WHERE c.c_nation = 'UNITED STATES'
  AND s.s_nation = 'UNITED STATES'
  AND d.d_year BETWEEN 1992 AND 1997
GROUP BY c.c_city, s.s_city, d.d_year
ORDER BY d.d_year, lo_revenue DESC;
