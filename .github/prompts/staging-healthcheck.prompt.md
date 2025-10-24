---
description: "Generate a staging healthcheck (types, null rules, sample stats) that writes results to ops/benchmarking/out/"
mode: 'agent'
---

You are in the **SQL Data Engineering** mode. Create or update:

1) `ops/benchmarking/staging_healthcheck.sql` that, for each of these objects:
   - stg_optimized.patients_std
   - stg_optimized.admissions_std
   - stg_optimized.billing_std
   - stg_optimized.diagnoses_std
   - stg_optimized.procedures_std
   - stg_optimized.lab_results_std
   - stg_optimized.providers_std
   - stg_optimized.departments_std
   - stg_optimized.hospitals_std
   runs:
   - `SELECT COUNT_BIG(1)` and `SELECT TOP (100) *` (ordered by clustered key).
   - 2–3 targeted aggregates per table (e.g., min/max dates; money sum tie-outs).
   - Writes metrics to `[ops].[benchmarks]` with columns: object_name, metric, value, run_utc, plan_id.
   - Emits a final `SELECT * FROM [ops].[benchmarks] WHERE run_utc >= DATEADD(HOUR,-1, SYSUTCDATETIME())` so results show in the grid.

2) `ops/benchmarking/README.md` explaining how to run and where CSVs appear.

Constraints:
- Use `TRY_CONVERT` only; **do not** change source objects.
- Make scripts idempotent (`IF OBJECT_ID(...) IS NULL` etc.).
- No long SQL in chat; apply file changes.
