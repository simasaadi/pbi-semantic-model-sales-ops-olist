# pbi-semantic-model-sales-ops-olist
[![CI (DuckDB pipeline + tests)](https://github.com/simasaadi/pbi-semantic-model-sales-ops-olist/actions/workflows/ci.yml/badge.svg)](https://github.com/simasaadi/pbi-semantic-model-sales-ops-olist/actions/workflows/ci.yml)
<!-- CI badge goes here (Step 2) -->

Builds a DuckDB analytics model (staging → typed staging → star schema mart) on the Olist dataset, with SQL-based data tests and a small performance benchmark.

## What’s inside
- **DuckDB pipeline**: loads raw CSVs into staging tables, types them, then builds a mart star schema
- **Data tests**: PK/FK integrity + value/range sanity checks
- **Perf**: before/after benchmark using `EXPLAIN ANALYZE` + indexes

## Project structure

data/
raw/ # raw Olist CSVs (CI pulls from GitHub Release)
processed/olist.duckdb # generated DuckDB database (local)
sql/
01_staging/
02_staging_typed/
03_model/
99_perf/
tests/
pipelines/
.github/workflows/

## How to run locally (Windows PowerShell)
```powershell
# 1) build DB + model
.\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\01_staging\load_staging.sql
.\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\02_staging_typed\stg_typed.sql
.\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f sql\03_model\build_star.sql

# 2) run tests (report)
.\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe data\processed\olist.duckdb -f tests\duckdb_test_report.sql
CI
GitHub Actions runs the same pipeline on Ubuntu:

downloads DuckDB CLI

downloads the raw dataset zip from a GitHub Release

unzips into data/raw

runs staging → typed → mart → tests

Performance benchmark
Benchmark query: year-month rollup for 2018.

Before indexes: 0.0085s

After indexes: 0.0057s

Improvement: ~33% faster

Indexes added:

mart.fact_order_item(purchase_date_key)

mart.fact_order_item(customer_id)

mart.fact_order_item(product_id)

mart.fact_order_item(seller_id)

mart.dim_date(date_key)

Notes
Raw dataset is kept out of Git history and supplied to CI via GitHub Releases.

The generated olist.duckdb is local output and should not be committed.
