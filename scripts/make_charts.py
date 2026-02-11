from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator


REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "processed" / "olist.duckdb"
OUT_DIR = REPO_ROOT / "docs" / "charts"
DPI = 220

plt.rcParams.update(
    {
        "figure.dpi": 120,
        "savefig.dpi": DPI,
        "font.size": 11,
        "axes.titlesize": 14,
        "axes.labelsize": 11,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "axes.grid": True,
        "grid.alpha": 0.22,
        "grid.linestyle": "-",
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)


def _ensure_outdir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def _connect() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found at:\n  {DB_PATH}\n\n"
            "Build it first (Windows PowerShell):\n"
            r"  .\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\01_staging\load_staging.sql" "\n"
            r"  .\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\02_staging_typed\stg_typed.sql" "\n"
            r"  .\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\03_model\build_star.sql" "\n"
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


def _fmt_currency_short(x: float, _pos) -> str:
    ax = abs(x)
    if ax >= 1_000_000_000:
        return f"${x/1_000_000_000:.1f}B"
    if ax >= 1_000_000:
        return f"${x/1_000_000:.1f}M"
    if ax >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:.0f}"


def _table_cols(con: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, str]:
    # returns {col_name_lower: TYPE_UPPER}
    info = con.execute(f"PRAGMA table_info('{table_name}')").df()
    return {str(r["name"]).lower(): str(r["type"]).upper() for _, r in info.iterrows()}


def _detect_date_expr(con: duckdb.DuckDBPyConnection) -> str:
    cols = _table_cols(con, "mart.dim_date")

    # Prefer date_key if it exists
    if "date_key" in cols:
        print(f"[make_charts] Using mart.dim_date.date_key ({cols['date_key']})")
        return "d.date_key"

    # Otherwise pick first date/timestamp-like column
    for name, typ in cols.items():
        if "DATE" in typ or "TIMESTAMP" in typ:
            print(f"[make_charts] Using mart.dim_date.{name} ({typ})")
            return f"d.{name}"

    raise RuntimeError("Could not find a usable date column in mart.dim_date.")


def _detect_revenue_expr(con: duckdb.DuckDBPyConnection) -> str:
    cols = _table_cols(con, "mart.fact_order_item")
    colset = set(cols.keys())

    # Your schema shows payment_total, item_price, freight_value
    if "payment_total" in colset:
        print("[make_charts] Using payment_total as revenue")
        return "f.payment_total"

    if "item_price" in colset and "freight_value" in colset:
        print("[make_charts] Using (item_price + freight_value) as revenue")
        return "(f.item_price + f.freight_value)"

    if "item_price" in colset:
        print("[make_charts] Using item_price as revenue")
        return "f.item_price"

    raise RuntimeError(
        "Could not determine revenue. Expected one of: payment_total, item_price(+freight_value), item_price.\n"
        f"Columns found: {sorted(colset)}"
    )


def _detect_category_expr(con: duckdb.DuckDBPyConnection) -> str:
    """
    Detect which category column exists in mart.dim_product.
    Your error shows: candidate 'category_en'
    """
    cols = _table_cols(con, "mart.dim_product")
    colset = set(cols.keys())

    # Most common possibilities
    candidates = [
        "category_en",
        "product_category_name",
        "product_category",
        "category",
        "category_name",
    ]
    for c in candidates:
        if c in colset:
            print(f"[make_charts] Using mart.dim_product.{c} for category")
            return f"p.{c}"

    raise RuntimeError(
        "Could not find a category column in mart.dim_product.\n"
        f"Columns found: {sorted(colset)}"
    )


def chart_revenue_by_month(con: duckdb.DuckDBPyConnection, date_expr: str, revenue_expr: str) -> None:
    q = f"""
    SELECT
      date_trunc('month', {date_expr}) AS month_start,
      SUM({revenue_expr})              AS revenue
    FROM mart.fact_order_item f
    JOIN mart.dim_date d
      ON d.date_key = f.purchase_date_key
    GROUP BY 1
    ORDER BY 1;
    """
    df = con.execute(q).df()
    df["month_start"] = pd.to_datetime(df["month_start"])
    df["rev_roll3"] = df["revenue"].rolling(3, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(11.5, 5.6))
    ax.plot(df["month_start"], df["revenue"], linewidth=2.2, marker="o", markersize=4, label="Monthly")
    ax.plot(df["month_start"], df["rev_roll3"], linewidth=2.2, linestyle="--", label="3M rolling avg")

    ax.set_title("Revenue by Month")
    ax.set_xlabel("")
    ax.set_ylabel("Revenue")
    ax.yaxis.set_major_formatter(FuncFormatter(_fmt_currency_short))
    ax.xaxis.set_major_locator(MaxNLocator(nbins=10))
    ax.legend(frameon=False, loc="upper left")

    fig.autofmt_xdate(rotation=35, ha="right")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "revenue_by_month.png", bbox_inches="tight")
    plt.close(fig)


def chart_top_categories(
    con: duckdb.DuckDBPyConnection,
    revenue_expr: str,
    category_expr: str,
    top_n: int = 15,
) -> None:
    q = f"""
    SELECT
      COALESCE(NULLIF(TRIM(CAST({category_expr} AS VARCHAR)), ''), 'unknown') AS category,
      SUM({revenue_expr})                                                    AS revenue
    FROM mart.fact_order_item f
    JOIN mart.dim_product p
      ON p.product_id = f.product_id
    GROUP BY 1
    ORDER BY revenue DESC
    LIMIT {top_n};
    """
    df = con.execute(q).df().sort_values("revenue", ascending=True)

    fig, ax = plt.subplots(figsize=(11.5, 7.0))
    ax.barh(df["category"], df["revenue"])
    ax.set_title(f"Top {top_n} Categories by Revenue")
    ax.set_xlabel("Revenue")
    ax.xaxis.set_major_formatter(FuncFormatter(_fmt_currency_short))
    ax.grid(True, axis="x", alpha=0.22)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "top_categories_revenue.png", bbox_inches="tight")
    plt.close(fig)


def chart_cohort_heatmap(con: duckdb.DuckDBPyConnection, date_expr: str, max_month: int = 12) -> None:
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
        date_trunc('month', {date_expr}) AS cohort_month
      FROM first_purchase fp
      JOIN mart.dim_date d
        ON d.date_key = fp.first_date_key
    ),
    activity AS (
      SELECT
        fm.cohort_month,
        date_trunc('month', {date_expr}) AS activity_month,
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
        COUNT(DISTINCT customer_id)                     AS active_customers
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
    pivot = (
        df.pivot(index="cohort_ym", columns="month_offset", values="active_customers")
        .fillna(0)
        .astype(int)
    )

    fig, ax = plt.subplots(figsize=(12.2, 7.2))
    im = ax.imshow(pivot.values, aspect="auto")

    ax.set_title(f"Cohort Activity (Active Customers) — Months 0–{max_month}")
    ax.set_xlabel("Months since first purchase")
    ax.set_ylabel("Cohort month (YYYY-MM)")

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([str(c) for c in pivot.columns])

    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(list(pivot.index))

    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Active customers")

    fig.tight_layout()
    fig.savefig(OUT_DIR / "cohort_activity_heatmap.png", bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    _ensure_outdir()
    con = _connect()
    try:
        date_expr = _detect_date_expr(con)
        revenue_expr = _detect_revenue_expr(con)
        category_expr = _detect_category_expr(con)

        chart_revenue_by_month(con, date_expr=date_expr, revenue_expr=revenue_expr)
        chart_top_categories(con, revenue_expr=revenue_expr, category_expr=category_expr, top_n=15)
        chart_cohort_heatmap(con, date_expr=date_expr, max_month=12)
    finally:
        con.close()

    print(f"[make_charts] Saved charts to: {OUT_DIR.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
