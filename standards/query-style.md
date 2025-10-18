# Query Style Guide

## CTE-First Structure

- Start complex queries with well-named CTEs ordered from rawest to most refined.
- Limit each CTE to a single responsibility (filtering, enrichment, aggregation) and comment when business logic is applied.
- Reference final results from a short `SELECT` statement at the bottom to keep execution plans readable.

## Join Practices

- Use explicit `INNER`/`LEFT`/`RIGHT` joins with `ON` clauses; avoid implicit comma joins.
- Qualify columns with table aliases (`los.encounter_id`) to prevent ambiguity.
- Keep aliases meaningful (`enc`, `lab`, `loc`) and document join cardinality when it differs from the expected one-to-one.
- Guard against accidental cross-joins by including all key predicates; add assertions in `sql/04_tests/` when duplication risk exists.

## Window Functions

- Prefer window functions for running totals, ranking, and lookbacks instead of correlated subqueries.
- Always specify `ORDER BY` and frame clauses to make intent explicit (e.g., `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`).
- When layering window functions, stage intermediate results in a CTE for clarity and reuse.

## Error Handling and Resilience

- Wrap destructive operations in transactions with explicit `BEGIN TRAN`/`ROLLBACK`/`COMMIT` when manipulating temp tables.
- Check row counts with `IF @@ROWCOUNT = 0` or data quality predicates and raise informative errors using `THROW`.
- Log execution metadata (run timestamp, source extracts) into audit tables when scheduled jobs run outside ad-hoc analysis.
- Avoid silent truncation by casting to the target data type before inserts.

## Common Pitfalls to Avoid

- Grouping without including all non-aggregated columns (causes unexpected duplication).
- Using `SELECT *` in production queries; enumerate columns to lock schema.
- Depending on implicit conversions between string and numeric types; cast explicitly.
- Neglecting filtered indexes or statistics updates on large staging tables.
- Forgetting to filter out soft-deleted records or test patients when computing KPIs.
