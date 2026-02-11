-- sql/03_model/build_star.sql
-- Build dimensional model (star schema) in DuckDB

CREATE SCHEMA IF NOT EXISTS mart;

-- 1) Dimension: Product Category (English)
CREATE OR REPLACE TABLE mart.dim_category AS
SELECT DISTINCT
  p.product_category_name AS category_pt,
  ct.product_category_name_english AS category_en
FROM stg_t.products p
LEFT JOIN stg_t.category_translation ct
  ON p.product_category_name = ct.product_category_name;

-- 2) Dimension: Product
CREATE OR REPLACE TABLE mart.dim_product AS
SELECT
  p.product_id,
  p.product_category_name AS category_pt,
  ct.product_category_name_english AS category_en,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,
  p.product_photos_qty
FROM stg_t.products p
LEFT JOIN stg_t.category_translation ct
  ON p.product_category_name = ct.product_category_name;

-- 3) Dimension: Seller
CREATE OR REPLACE TABLE mart.dim_seller AS
SELECT
  seller_id,
  seller_city,
  seller_state,
  seller_zip_code_prefix
FROM stg_t.sellers;

-- 4) Dimension: Customer
CREATE OR REPLACE TABLE mart.dim_customer AS
SELECT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state,
  customer_zip_code_prefix
FROM stg_t.customers;

-- 5) Dimension: Date (from min->max purchase date)
CREATE OR REPLACE TABLE mart.dim_date AS
WITH bounds AS (
  SELECT
    CAST(MIN(order_purchase_ts) AS DATE) AS min_d,
    CAST(MAX(order_purchase_ts) AS DATE) AS max_d
  FROM stg_t.orders
),
dates AS (
  SELECT * FROM generate_series((SELECT min_d FROM bounds), (SELECT max_d FROM bounds), INTERVAL 1 DAY)
)
SELECT
  CAST(generate_series AS DATE) AS date_key,
  EXTRACT(YEAR FROM generate_series) AS year,
  EXTRACT(MONTH FROM generate_series) AS month,
  EXTRACT(DAY FROM generate_series) AS day,
  EXTRACT(DOW FROM generate_series) AS day_of_week,
  STRFTIME(generate_series, '%Y-%m') AS year_month
FROM dates;

-- 6) Helper aggregates (payments + reviews at order level)
CREATE OR REPLACE VIEW mart._order_payment AS
SELECT
  order_id,
  SUM(payment_value) AS payment_total,
  COUNT(*) AS payment_rows
FROM stg_t.order_payments
GROUP BY 1;

CREATE OR REPLACE VIEW mart._order_review AS
SELECT
  order_id,
  AVG(review_score) AS review_score_avg,
  MIN(review_creation_ts) AS first_review_created_ts
FROM stg_t.order_reviews
GROUP BY 1;

-- 7) FACT: Order Items (grain = order_id + order_item_id)
CREATE OR REPLACE TABLE mart.fact_order_item AS
SELECT
  oi.order_id,
  oi.order_item_id,
  o.customer_id,
  oi.product_id,
  oi.seller_id,

  CAST(o.order_purchase_ts AS DATE) AS purchase_date_key,
  o.order_status,

  oi.shipping_limit_ts,
  o.order_approved_ts,
  o.order_delivered_carrier_ts,
  o.order_delivered_customer_ts,
  o.order_estimated_delivery_ts,

  oi.price AS item_price,
  oi.freight_value AS freight_value,

  p.payment_total,
  r.review_score_avg

FROM stg_t.order_items oi
JOIN stg_t.orders o
  ON oi.order_id = o.order_id
LEFT JOIN mart._order_payment p
  ON oi.order_id = p.order_id
LEFT JOIN mart._order_review r
  ON oi.order_id = r.order_id;

-- 8) Sanity checks
-- Fact rowcount should match stg_t.order_items rowcount
SELECT
  (SELECT COUNT(*) FROM stg_t.order_items) AS stg_order_items,
  (SELECT COUNT(*) FROM mart.fact_order_item) AS fact_order_items;

-- Basic null checks for keys
SELECT
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_id
FROM mart.fact_order_item;
