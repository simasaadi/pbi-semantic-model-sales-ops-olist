from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "processed" / "olist.duckdb"
OUT_DIR = REPO_ROOT / "docs" / "charts"

# Output quality
DPI = 220


def _ensure_outdir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def _fmt_millions(x, _pos):
    # 1_500_000 -> "1.50M"
    return f"{x/1_000_000:.2f}M"


def _connect() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found at {DB_PATH}\n\n"
            "Build it first (local):\n"
            r"  .\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\01_staging\load_staging.sql" "\n"
            r"  .\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\02_staging_typed\stg_typed.sql" "\n"
            r"  .\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\03_model\build_star.sql" "\n"
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


def chart_revenue_by_month(con: duckdb.DuckDBPyConnection) -> None:
    q = """
    SELECT
      strftime('%Y-%m', d.full_date) AS ym,
      SUM(f.revenue)                AS revenue
    FROM mart.fact_order_item f
    JOIN mart.dim_date d
      ON d.date_key = f.purchase_date_key
    GROUP BY 1
    ORDER BY 1;
    """
    df = con.execute(q).df()
    df["ym"] = pd.to_datetime(df["ym"] + "-01")

    fig, ax = plt.subplots(figsize=(10.5, 5.5))
    ax.plot(df["ym"], df["revenue"], marker="o", linewidth=2)
    ax.set_title("Revenue by Month", pad=12)
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue")
    ax.yaxis.set_major_formatter(FuncFormatter(_fmt_millions))
    ax.grid(True, linestyle=":", linewidth=1, alpha=0.6)
    fig.autofmt_xdate(rotation=45, ha="right")
    fig.tight_layout()

    fig.savefig(OUT_DIR / "revenue_by_month.png", dpi=DPI, bbox_inches="tight")
    plt.close(fig)


def chart_top_categories(con: duckdb.DuckDBPyConnection, top_n: int = 15) -> None:
    q = f"""
    SELECT
      p.product_category_name AS category,
      SUM(f.revenue)          AS revenue
    FROM mart.fact_order_item f
    JOIN mart.dim_product p
      ON p.product_id = f.product_id
    GROUP BY 1
    ORDER BY revenue DESC
    LIMIT {top_n};
    """
    df = con.execute(q).df()

    # Make it easier to read: horizontal bars, smallest at bottom
    df = df.sort_values("revenue", ascending=True)

    fig, ax = plt.subplots(figsize=(10.5, 6.5))
    ax.barh(df["category"], df["revenue"])
    ax.set_title(f"Top Categories by Revenue (Top {top_n})", pad=12)
    ax.set_xlabel("Revenue")
    ax.xaxis.set_major_formatter(FuncFormatter(_fmt_millions))
    ax.grid(True, axis="x", linestyle=":", linewidth=1, alpha=0.6)
    fig.tight_layout()

    fig.savefig(OUT_DIR / "top_categories_revenue.png", dpi=DPI, bbox_inches="tight")
    plt.close(fig)


def chart_cohort_heatmap(con: duckdb.DuckDBPyConnection, max_month: int = 12) -> None:
    """
    Cohort definition: customer first purchase month.
    Activity: count of distinct customers active in cohort_month + offset month.
    """
    q = f"""
    WITH first_purchase AS (
      SELECT
        customer_id,
        MIN(purchase_date_key) AS first_date_key
      FROM mart.fact_order_item
      GROUP BY 1
    ),
    first_month AS (
      SELECT
        fp.customer_id,
        date_trunc('month', d.full_date) AS cohort_month
      FROM first_purchase fp
      JOIN mart.dim_date d
        ON d.date_key = fp.first_date_key
    ),
    activity AS (
      SELECT
        fm.cohort_month,
        date_trunc('month', d.full_date) AS activity_month,
        f.customer_id
      FROM mart.fact_order_item f
      JOIN mart.dim_date d
        ON d.date_key = f.purchase_date_key
      JOIN first_month fm
        ON fm.customer_id = f.customer_id
    ),
    offsets AS (
      SELECT
        cohort_month,
        datediff('month', cohort_month, activity_month) AS month_offset,
        COUNT(DISTINCT customer_id) AS active_customers
      FROM activity
      GROUP BY 1, 2
      HAVING month_offset BETWEEN 0 AND {max_month}
    )
    SELECT
      strftime('%Y-%m', cohort_month) AS cohort_ym,
      month_offset,
      active_customers
    FROM offsets
    ORDER BY cohort_month, month_offset;
    """
    df = con.execute(q).df()

    # Pivot into a matrix: rows = cohort, cols = month_offset
    pivot = df.pivot(index="cohort_ym", columns="month_offset", values="active_customers").fillna(0)

    fig, ax = plt.subplots(figsize=(11, 7))
    im = ax.imshow(pivot.values, aspect="auto")

    ax.set_title(f"Cohort Activity (Active Customers) — Months 0–{max_month}", pad=12)
    ax.set_xlabel("Months since first purchase")
    ax.set_ylabel("Cohort month (YYYY-MM)")

    # X ticks: 0..max_month
    ax.set_xticks(list(range(pivot.shape[1])))
    ax.set_xticklabels([str(c) for c in pivot.columns])

    # Y ticks: every row, but don’t crowd
    yticks = list(range(pivot.shape[0]))
    ax.set_yticks(yticks)
    ax.set_yticklabels(pivot.index)

    # Colorbar
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Active customers")

    fig.tight_layout()
    fig.savefig(OUT_DIR / "cohort_activity_heatmap.png", dpi=DPI, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    _ensure_outdir()
    con = _connect()
    try:
        chart_revenue_by_month(con)
        chart_top_categories(con, top_n=15)
        chart_cohort_heatmap(con, max_month=12)
    finally:
        con.close()

    print(f"Saved charts to {OUT_DIR.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
