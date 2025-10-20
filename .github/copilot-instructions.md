# Copilot — SQL Data Engineering guardrails

**General**
- Always CREATE or UPDATE files in this workspace; do NOT dump long SQL in chat. Prefer Edit or Agent mode to apply changes across files.
- Use the project structure:
  `01_staging_optimized/` (materialized tables + thin views), `02_transform/` (semantic models, dims/facts/KPIs), `tests/tsqlt/` (unit tests), `ops/benchmarking/` (timing & healthchecks), `docs/` (README & data dictionary).
- Every change must be idempotent: guard with `IF NOT EXISTS` or `CREATE OR ALTER`.

**Structural rules**
- Never re-create `sql/01_staging/`; optimized staging is canonical.
- Keep heavy transformations out of staging; move them into `sql/02_transform/`.
- When in doubt, default to transform before KPI; staging stays thin.

**Staging rules**
- Heavy cleanup happens ONCE into materialized tables under schema `stg_optimized` (suffix `_tbl`). Expose **thin views** (no heavy parsing) with suffix `_vw`. If a view already exists and is slow, propose creating a materialized table and re-point the view.
- Data types at rest: use native types (e.g., `DATE`, `DATETIME2`, `INT`, `DECIMAL(19,2)` for money). Never keep dates/money as NVARCHAR.
- Conversions: prefer `TRY_CONVERT/TRY_CAST` with styles; avoid `TRY_PARSE/FORMAT`.
- Normalize shapes only (e.g., CPT/ICD/LOINC strings, zero-padding) in staging; **semantic mappings** to reference catalogs happen in `02_transform`.
- Null handling: follow table-specific prompts; only use `'NA'` where explicitly required, else `NULL`.
- Performance: for large tables (labs/procedures/diagnoses/billing), add **nonclustered columnstore indexes** (NCCI) and date partitioning where appropriate; add minimal B-tree indexes for joins.
- Persisted computed columns are allowed only if deterministic; consider indexing them.

**Transform rules**
- Join to reference catalogs (ICD-10, LOINC, UCUM; CPT format only) and apply business logic (readmissions 30-day, bed-days explosion, Pareto, KPIs).
- Deliver re-usable SQL in `02_transform/*.sql` and document caveats in `docs/`.

**Testing & benchmarking**
- Create tSQLt tests under `tests/tsqlt/` for: types/domains, referential checks, arithmetic tie-outs (e.g., `patient_due = total - insurance`), date logic windows.
- Add `ops/benchmarking/*` to measure: rowcount, `TOP (100)` sample, and a few targeted aggregates. Output to `[ops].[benchmarks]` table and CSVs in `/ops/benchmarking/out/`.

**Behavior**
- When I give you a “prompt spec”, generate or modify the correct **files** (multiple files if needed), then summarize what changed. No long SQL in chat; commit-ready files only.
