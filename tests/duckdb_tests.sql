-- tests/duckdb_tests.sql
-- Simple data quality tests for mart schema
-- Convention: each test returns 0 rows when PASS, >0 rows when FAIL

-- 1) Fact primary key uniqueness (order_id + order_item_id)
SELECT
  order_id,
  order_item_id,
  COUNT(*) AS dupes
FROM mart.fact_order_item
GROUP BY 1,2
HAVING COUNT(*) > 1;

-- 2) No negative money
SELECT *
FROM mart.fact_order_item
WHERE item_price < 0
   OR freight_value < 0
   OR payment_total < 0;

-- 3) Date key must exist in dim_date
SELECT f.purchase_date_key
FROM mart.fact_order_item f
LEFT JOIN mart.dim_date d
  ON f.purchase_date_key = d.date_key
WHERE d.date_key IS NULL
GROUP BY 1;

-- 4) Customer FK must exist
SELECT f.customer_id
FROM mart.fact_order_item f
LEFT JOIN mart.dim_customer c
  ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL
GROUP BY 1;

-- 5) Product FK must exist
SELECT f.product_id
FROM mart.fact_order_item f
LEFT JOIN mart.dim_product p
  ON f.product_id = p.product_id
WHERE p.product_id IS NULL
GROUP BY 1;

-- 6) Seller FK must exist
SELECT f.seller_id
FROM mart.fact_order_item f
LEFT JOIN mart.dim_seller s
  ON f.seller_id = s.seller_id
WHERE s.seller_id IS NULL
GROUP BY 1;

-- 7) Reasonable review score range (ignore NULLs)
SELECT review_score_avg
FROM mart.fact_order_item
WHERE review_score_avg IS NOT NULL
  AND (review_score_avg < 1 OR review_score_avg > 5)
GROUP BY 1;
