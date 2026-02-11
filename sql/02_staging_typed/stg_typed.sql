-- sql/02_staging_typed/stg_typed.sql
-- Typed staging views: cast columns to appropriate types

CREATE SCHEMA IF NOT EXISTS stg_t;

-- Helpers: safe casting for numbers
-- (DuckDB supports TRY_CAST; returns NULL instead of error)

CREATE OR REPLACE VIEW stg_t.customers AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM stg.customers;

CREATE OR REPLACE VIEW stg_t.orders AS
SELECT
  order_id,
  customer_id,
  order_status,
  TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
  TRY_CAST(order_approved_at AS TIMESTAMP)        AS order_approved_ts,
  TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_ts,
  TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_ts,
  TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_ts
FROM stg.orders;

CREATE OR REPLACE VIEW stg_t.order_items AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
  TRY_CAST(price AS DOUBLE) AS price,
  TRY_CAST(freight_value AS DOUBLE) AS freight_value
FROM stg.order_items;

CREATE OR REPLACE VIEW stg_t.order_payments AS
SELECT
  order_id,
  payment_sequential,
  payment_type,
  TRY_CAST(payment_installments AS INTEGER) AS payment_installments,
  TRY_CAST(payment_value AS DOUBLE) AS payment_value
FROM stg.order_payments;

CREATE OR REPLACE VIEW stg_t.order_reviews AS
SELECT
  review_id,
  order_id,
  TRY_CAST(review_score AS INTEGER) AS review_score,
  TRY_CAST(review_creation_date AS TIMESTAMP) AS review_creation_ts,
  TRY_CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts
FROM stg.order_reviews;

CREATE OR REPLACE VIEW stg_t.products AS
SELECT
  product_id,
  product_category_name,
  TRY_CAST(product_name_lenght AS INTEGER) AS product_name_length,
  TRY_CAST(product_description_lenght AS INTEGER) AS product_description_length,
  TRY_CAST(product_photos_qty AS INTEGER) AS product_photos_qty,
  TRY_CAST(product_weight_g AS INTEGER) AS product_weight_g,
  TRY_CAST(product_length_cm AS INTEGER) AS product_length_cm,
  TRY_CAST(product_height_cm AS INTEGER) AS product_height_cm,
  TRY_CAST(product_width_cm AS INTEGER) AS product_width_cm
FROM stg.products;

CREATE OR REPLACE VIEW stg_t.sellers AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM stg.sellers;

CREATE OR REPLACE VIEW stg_t.category_translation AS
SELECT
  product_category_name,
  product_category_name_english
FROM stg.category_translation;

-- Geolocation: keep as-is (huge table). We'll aggregate later.
CREATE OR REPLACE VIEW stg_t.geolocation AS
SELECT
  geolocation_zip_code_prefix,
  TRY_CAST(geolocation_lat AS DOUBLE) AS geolocation_lat,
  TRY_CAST(geolocation_lng AS DOUBLE) AS geolocation_lng,
  geolocation_city,
  geolocation_state
FROM stg.geolocation;

-- Sanity checks: timestamp parse success rate
SELECT
  'orders' AS table_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN order_purchase_ts IS NULL THEN 1 ELSE 0 END) AS null_purchase_ts
FROM stg_t.orders;
