-- sql/99_perf/add_indexes.sql

-- Fact table join/filter helpers
CREATE INDEX IF NOT EXISTS idx_fact_purchase_date_key ON mart.fact_order_item(purchase_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer_id       ON mart.fact_order_item(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_product_id        ON mart.fact_order_item(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_seller_id         ON mart.fact_order_item(seller_id);

-- Dimension keys (usually already good, but explicit is fine)
CREATE INDEX IF NOT EXISTS idx_dim_date_key ON mart.dim_date(date_key);
