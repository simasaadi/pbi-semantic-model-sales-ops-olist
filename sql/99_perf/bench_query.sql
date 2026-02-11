-- sql/99_perf/bench_query.sql
-- A realistic analytics query to benchmark

EXPLAIN ANALYZE
SELECT
  d.year,
  d.month,
  COUNT(*) AS order_items,
  SUM(f.payment_total) AS revenue
FROM mart.fact_order_item f
JOIN mart.dim_date d
  ON f.purchase_date_key = d.date_key
WHERE d.year = 2018
GROUP BY 1,2
ORDER BY 1,2;
