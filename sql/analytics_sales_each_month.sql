SELECT TO_CHAR(s.sales_period, 'YYYYMM') AS "period (YYYYMM)",
o.outlet_code,
o.outlet_name,
SUM(s.actual_sales) AS sales
FROM sales s
INNER JOIN outlets o ON s.outlet_code = o.outlet_code
GROUP BY s.sales_period, o.outlet_code, o.outlet_name
ORDER BY s.sales_period, o.outlet_code;
