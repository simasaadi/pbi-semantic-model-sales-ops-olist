-- tests/duckdb_test_report.sql
-- Produces a single summary table and throws an error if any test fails.

CREATE SCHEMA IF NOT EXISTS qa;

DROP TABLE IF EXISTS qa.test_results;
CREATE TABLE qa.test_results (
  test_name VARCHAR,
  failing_rows BIGINT
);

-- Helper pattern:
-- Insert test_name + count(*) of failing rows

INSERT INTO qa.test_results
SELECT 'fact_pk_unique (order_id, order_item_id)', COUNT(*)
FROM (
  SELECT order_id, order_item_id
  FROM mart.fact_order_item
  GROUP BY 1,2
  HAVING COUNT(*) > 1
);

INSERT INTO qa.test_results
SELECT 'no_negative_money', COUNT(*)
FROM mart.fact_order_item
WHERE item_price < 0 OR freight_value < 0 OR payment_total < 0;

INSERT INTO qa.test_results
SELECT 'purchase_date_key_exists_in_dim_date', COUNT(*)
FROM (
  SELECT f.purchase_date_key
  FROM mart.fact_order_item f
  LEFT JOIN mart.dim_date d ON f.purchase_date_key = d.date_key
  WHERE d.date_key IS NULL
);

INSERT INTO qa.test_results
SELECT 'customer_fk_exists', COUNT(*)
FROM (
  SELECT f.customer_id
  FROM mart.fact_order_item f
  LEFT JOIN mart.dim_customer c ON f.customer_id = c.customer_id
  WHERE c.customer_id IS NULL
);

INSERT INTO qa.test_results
SELECT 'product_fk_exists', COUNT(*)
FROM (
  SELECT f.product_id
  FROM mart.fact_order_item f
  LEFT JOIN mart.dim_product p ON f.product_id = p.product_id
  WHERE p.product_id IS NULL
);

INSERT INTO qa.test_results
SELECT 'seller_fk_exists', COUNT(*)
FROM (
  SELECT f.seller_id
  FROM mart.fact_order_item f
  LEFT JOIN mart.dim_seller s ON f.seller_id = s.seller_id
  WHERE s.seller_id IS NULL
);

INSERT INTO qa.test_results
SELECT 'review_score_avg_in_1_to_5', COUNT(*)
FROM mart.fact_order_item
WHERE review_score_avg IS NOT NULL
  AND (review_score_avg < 1 OR review_score_avg > 5);

-- Show the report
SELECT
  test_name,
  failing_rows,
  CASE WHEN failing_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM qa.test_results
ORDER BY status DESC, failing_rows DESC, test_name;

-- Fail loudly if any test failed
SELECT
  CASE
    WHEN SUM(CASE WHEN failing_rows > 0 THEN 1 ELSE 0 END) > 0
    THEN error('QA tests failed. See qa.test_results.')
    ELSE 1
  END AS ok
FROM qa.test_results;
