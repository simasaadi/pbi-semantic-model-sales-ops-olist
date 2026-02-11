# pbi-semantic-model-sales-ops-olist



\## Performance improvement (DuckDB indexes + EXPLAIN ANALYZE)



Benchmark query (year-month rollup for 2018):



\- Before indexes: 0.0085s total time

\- After indexes:  0.0057s total time

\- Improvement: ~33% faster



Indexes added:

\- mart.fact\_order\_item(purchase\_date\_key)

\- mart.fact\_order\_item(customer\_id)

\- mart.fact\_order\_item(product\_id)

\- mart.fact\_order\_item(seller\_id)

\- mart.dim\_date(date\_key)



