-- sql/01_staging/load_staging.sql
-- Load raw Olist CSVs into DuckDB staging tables

CREATE SCHEMA IF NOT EXISTS stg;

-- Customers
CREATE OR REPLACE TABLE stg.customers AS
SELECT * FROM read_csv_auto('data/raw/olist_customers_dataset.csv', ALL_VARCHAR=TRUE);

-- Geolocation (large)
CREATE OR REPLACE TABLE stg.geolocation AS
SELECT * FROM read_csv_auto('data/raw/olist_geolocation_dataset.csv', ALL_VARCHAR=TRUE);

-- Orders
CREATE OR REPLACE TABLE stg.orders AS
SELECT * FROM read_csv_auto('data/raw/olist_orders_dataset.csv', ALL_VARCHAR=TRUE);

-- Order Items
CREATE OR REPLACE TABLE stg.order_items AS
SELECT * FROM read_csv_auto('data/raw/olist_order_items_dataset.csv', ALL_VARCHAR=TRUE);

-- Payments
CREATE OR REPLACE TABLE stg.order_payments AS
SELECT * FROM read_csv_auto('data/raw/olist_order_payments_dataset.csv', ALL_VARCHAR=TRUE);

-- Reviews
CREATE OR REPLACE TABLE stg.order_reviews AS
SELECT * FROM read_csv_auto('data/raw/olist_order_reviews_dataset.csv', ALL_VARCHAR=TRUE);

-- Products
CREATE OR REPLACE TABLE stg.products AS
SELECT * FROM read_csv_auto('data/raw/olist_products_dataset.csv', ALL_VARCHAR=TRUE);

-- Sellers
CREATE OR REPLACE TABLE stg.sellers AS
SELECT * FROM read_csv_auto('data/raw/olist_sellers_dataset.csv', ALL_VARCHAR=TRUE);

-- Category translations
CREATE OR REPLACE TABLE stg.category_translation AS
SELECT * FROM read_csv_auto('data/raw/product_category_name_translation.csv', ALL_VARCHAR=TRUE);

-- Quick row-count sanity checks
SELECT 'customers' AS table_name, COUNT(*) AS rows FROM stg.customers
UNION ALL SELECT 'geolocation', COUNT(*) FROM stg.geolocation
UNION ALL SELECT 'orders', COUNT(*) FROM stg.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM stg.order_items
UNION ALL SELECT 'order_payments', COUNT(*) FROM stg.order_payments
UNION ALL SELECT 'order_reviews', COUNT(*) FROM stg.order_reviews
UNION ALL SELECT 'products', COUNT(*) FROM stg.products
UNION ALL SELECT 'sellers', COUNT(*) FROM stg.sellers
UNION ALL SELECT 'category_translation', COUNT(*) FROM stg.category_translation
ORDER BY table_name;
