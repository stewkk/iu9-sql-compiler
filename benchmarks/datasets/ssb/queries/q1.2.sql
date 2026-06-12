SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM lineorder AS lo
JOIN date AS d ON lo.lo_orderdate = d.d_datekey
WHERE d.d_yearmonthnum = 199401
  AND lo.lo_discount BETWEEN 4 AND 6
  AND lo.lo_quantity BETWEEN 26 AND 35;
