SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM lineorder AS lo
JOIN date AS d ON lo.lo_orderdate = d.d_datekey
WHERE d.d_year = 1993
  AND lo.lo_discount BETWEEN 1 AND 3
  AND lo.lo_quantity < 25;
