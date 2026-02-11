import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = os.path.join("data", "processed", "olist.duckdb")
OUT_DIR = os.path.join("docs", "charts")

os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect(DB_PATH, read_only=True)

def save_line(df, x, y, title, filename):
    plt.figure()
    plt.plot(df[x], df[y])
    plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, filename), dpi=200)
    plt.close()

def save_barh(df, x, y, title, filename):
    plt.figure()
    plt.barh(df[y], df[x])
    plt.title(title)
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, filename), dpi=200)
    plt.close()

# 1) Revenue by month (advanced enough + clear)
q_monthly = """
SELECT
  d.year_month,
  SUM(f.payment_total) AS revenue
FROM mart.fact_order_item f
JOIN mart.dim_date d
  ON f.purchase_date_key = d.date_key
GROUP BY 1
ORDER BY 1;
"""
df_monthly = con.execute(q_monthly).df()
save_line(df_monthly, "year_month", "revenue", "Revenue by Month", "revenue_by_month.png")

# 2) Top 15 categories by revenue
q_cat = """
SELECT
  COALESCE(p.category_en, p.category_pt, 'unknown') AS category,
  SUM(f.payment_total) AS revenue
FROM mart.fact_order_item f
JOIN mart.dim_product p
  ON f.product_id = p.product_id
GROUP BY 1
ORDER BY revenue DESC
LIMIT 15;
"""
df_cat = con.execute(q_cat).df().sort_values("revenue", ascending=True)
save_barh(df_cat, "revenue", "category", "Top Categories by Revenue (Top 15)", "top_categories_revenue.png")

# 3) Cohort retention heatmap data (we’ll export as a table image for now)
# Cohort = customer first purchase month; retention = months since first purchase
q_cohort = """
WITH first_purchase AS (
  SELECT
    customer_id,
    MIN(purchase_date_key) AS first_date
  FROM mart.fact_order_item
  GROUP BY 1
),
activity AS (
  SELECT DISTINCT
    f.customer_id,
    f.purchase_date_key
  FROM mart.fact_order_item f
),
cohort_activity AS (
  SELECT
    STRFTIME(fp.first_date, '%Y-%m') AS cohort_month,
    DATE_DIFF('month', fp.first_date, a.purchase_date_key) AS months_since,
    a.customer_id
  FROM activity a
  JOIN first_purchase fp
    ON a.customer_id = fp.customer_id
)
SELECT
  cohort_month,
  months_since,
  COUNT(DISTINCT customer_id) AS active_customers
FROM cohort_activity
WHERE months_since BETWEEN 0 AND 12
GROUP BY 1,2
ORDER BY 1,2;
"""
df_cohort = con.execute(q_cohort).df()

# Pivot for a heatmap-like table image
pivot = df_cohort.pivot(index="cohort_month", columns="months_since", values="active_customers").fillna(0).astype(int)
plt.figure(figsize=(10, 6))
plt.imshow(pivot.values, aspect="auto")
plt.title("Cohort Activity (Count of Active Customers) — Months 0–12")
plt.yticks(range(len(pivot.index)), pivot.index)
plt.xticks(range(len(pivot.columns)), pivot.columns)
plt.tight_layout()
plt.savefig(os.path.join(OUT_DIR, "cohort_activity_heatmap.png"), dpi=200)
plt.close()

print(f"Saved charts to {OUT_DIR}")
