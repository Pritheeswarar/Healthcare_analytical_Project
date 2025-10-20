# Healthcare SQL Project Structure

This document inventories the repository tree (excluding local virtual environments) and summarizes the purpose of each tracked path.

## Root

- `.sqlfluff` — SQLFluff configuration targeting the T-SQL dialect.
- `README.md` — Onboarding guide, layer responsibilities, and tooling expectations.
- `CONTRIBUTING.md` — Reminder to run SQLFluff lint with the T-SQL dialect before committing.
- `docs/DEPRECATIONS.md` — Catalog of removed assets and rationale.

## .github

- `.github/copilot-instructions.md` — Guardrails for Copilot usage, including staging/transform separation rules.
- `.github/chatmodes/sql-data-engineering.chatmode.md` — Chat mode descriptor enforcing SQL data engineering workflows.
- `.github/prompts/staging-healthcheck.prompt.md` — Prompt specification for the staging health check benchmark script.
- `.github/prompts/timing-runner.prompt.md` — Prompt for the timing runner harness that logs query performance.
- `.github/prompts/tsqlt-scaffold.prompt.md` — Prompt instructions for scaffolding tSQLt-based regression tests.

## .vscode

- `.vscode/extensions.json` — Recommended VS Code extensions (MSSQL, SQLFluff, Copilot).
- `.vscode/settings.json` — Workspace preferences for SQL editing, formatting, and file associations.

## docs

- `docs/data-governance.md` — HIPAA Safe Harbor guidance, identifier handling, date masking strategy, and analyst checklist.
- `docs/DEPRECATIONS.md` — Tracking document for retired assets (e.g., legacy staging views).

## ops/benchmarking

- `ops/benchmarking/README.md` — Usage notes for health checks and timing harness.
- `ops/benchmarking/agent_job_timing.sql` — SQL Agent job script wiring the timing runner into a nightly schedule.
- `ops/benchmarking/staging_healthcheck.sql` — Benchmarks `stg_optimized` views and logs metrics into `ops.benchmarks`.
- `ops/benchmarking/timing_runner.sql` — MAXDOP-capped timing harness persisting results to `ops.benchmarks`.

## perf

- `perf/README.md` — Overview of performance diagnostics and rewrite experiments.
- `perf/analytics_patients_pipeline.sql` — Incremental materialization workflow for an analytics-ready patients dimension.
- `perf/baseline.md` — Recorded STATISTICS IO/TIME baselines highlighting slow staging patterns.
- `perf/diagnostics.sql` — Diagnostic harness collecting DMV snapshots, statistics output, and representative plans.
- `perf/findings.md` — Catalog of performance anti-patterns with evidence and recommendations.
- `perf/index_recommendations.sql` — Deferred nonclustered index definitions for future materialized staging tables.
- `perf/staging_decision.md` — Decision memo comparing view-only staging versus materialized tables and indexed views.
- `perf/stats_maintenance.sql` — Manual statistics maintenance script for volatile source tables.

### perf/rewrites

- `perf/rewrites/patients_std_materialization.sql` — Experimental materialization script for a cleansed patients table.
- `perf/rewrites/patients_std_rewrite.sql` — Stub illustrating the intended post-materialization select pattern.

## sql/00_metadata

- `sql/00_metadata/00_info_schema_inventory.sql` — INFORMATION_SCHEMA-based column inventory extractor.
- `sql/00_metadata/01_table_rowcounts.sql` — Partition stats-driven table row count summary.
- `sql/00_metadata/02_profile_columns_template.sql` — Parameterized column profiling template.

### sql/00_metadata/Query Results

- `sql/00_metadata/Query Results/admissions_column_profiling.csv` — Export of column profiling metrics for admissions.
- `sql/00_metadata/Query Results/billing_column_profiling.csv` — Column profiling results for billing.
- `sql/00_metadata/Query Results/Data dictionary with standardization notes.csv` — Narrative data dictionary with standardization recommendations.
- `sql/00_metadata/Query Results/departments_column_profiling.csv` — Profiling output for department attributes.
- `sql/00_metadata/Query Results/diagnoses_column_profiling.csv` — Profiling output for diagnoses fields.
- `sql/00_metadata/Query Results/hospitals_column_profiling.csv` — Profiling output for hospital reference data.
- `sql/00_metadata/Query Results/lab_results_column_profiling.csv` — Profiling output for laboratory results.
- `sql/00_metadata/Query Results/patients_column_profiling.csv` — Profiling output for patient demographics.
- `sql/00_metadata/Query Results/procedures_column_profiling.csv` — Profiling output for procedures.
- `sql/00_metadata/Query Results/providers_column_profiling.csv` — Profiling output for providers.
- `sql/00_metadata/Query Results/Results_00_info_schema_inventory.csv` — Saved results from the INFORMATION_SCHEMA inventory query.
- `sql/00_metadata/Query Results/Results_01_table_rowcounts.csv` — Saved results from the table row count inventory.

## sql/01_staging_optimized

- `sql/01_staging_optimized/README.md` — Guidance for maintaining thin, type-enforcing staging views.
- `sql/01_staging_optimized/admissions_std.sql` — Optimized admissions view built for `stg_optimized`.
- `sql/01_staging_optimized/billing_std.sql` — Optimized billing view emphasizing typed monetary columns.
- `sql/01_staging_optimized/departments_std.sql` — Optimized departments view providing typed occupancy metrics.
- `sql/01_staging_optimized/diagnoses_std.sql` — Optimized diagnoses view with constrained date parsing styles.
- `sql/01_staging_optimized/hospitals_std.sql` — Optimized hospitals view exposing normalized facility attributes.
- `sql/01_staging_optimized/lab_results_std.sql` — Optimized lab results view that normalizes result units and dates.
- `sql/01_staging_optimized/patients_std.sql` — Optimized patients view using token staging CTEs with minimal branching.
- `sql/01_staging_optimized/procedures_std.sql` — Optimized procedures view with structured parsing for CPT codes.
- `sql/01_staging_optimized/providers_std.sql` — Optimized providers view delivering cleaned specialties and experience metrics.

## sql/02_transform

- `sql/02_transform/admissions_xf.sql` — Stub for admissions entity conformance.
- `sql/02_transform/billing_xf.sql` — Stub for billing entity conformance.
- `sql/02_transform/departments_xf.sql` — Stub for departments entity conformance.
- `sql/02_transform/diagnoses_xf.sql` — Stub for diagnoses entity conformance.
- `sql/02_transform/hospitals_xf.sql` — Stub for hospitals entity conformance.
- `sql/02_transform/lab_results_xf.sql` — Stub for lab results entity conformance.
- `sql/02_transform/patients_xf.sql` — Stub for patients entity conformance.
- `sql/02_transform/procedures_xf.sql` — Stub for procedures entity conformance.
- `sql/02_transform/providers_xf.sql` — Stub for providers entity conformance.

## sql/03_kpis

- `sql/03_kpis/03_kpis_admissions.sql` — Placeholder for LOS and admissions trend metrics.
- `sql/03_kpis/03_kpis_occupancy.sql` — Placeholder for occupancy calculations (patient-days / beds × days).
- `sql/03_kpis/03_kpis_revenue.sql` — Placeholder for billed vs paid vs patient_due tie-out metrics.
- `sql/03_kpis/03_kpis_top_codes.sql` — Placeholder for ICD/CPT frequency and Pareto analysis.
- `sql/03_kpis/03_kpis_readmissions.sql` — Placeholder for 30-day readmission metrics.
- `sql/03_kpis/03_kpis_labs.sql` — Placeholder for lab completion and turnaround metrics.

## ref

- `ref/department.sql` — Canonical department metadata table stub.
- `ref/department_map.sql` — Mapping between source department codes and canonical department identifiers.
- `ref/cpt_map.sql` — CPT code reference stub.
- `ref/icd_map.sql` — ICD code reference stub.
- `ref/gender_map.sql` — Mapping between source gender values and standardized labels.
- `ref/insurance_map.sql` — Mapping between source payer names and standardized insurance classes.
- `ref/unit_map.sql` — Mapping between source unit identifiers and standardized department units.

## standards

- `standards/naming-conventions.md` — Naming policy for schemas, tables, indexes, and header templates.
- `standards/query-style.md` — Query style guide covering CTE usage, join patterns, window functions, and error handling.

## tests

- `tests/README.md` — Instructions for running the tSQLt harness locally.
- `tests/bootstrap.sql` — Comment-only script outlining steps to install tSQLt and scaffold test classes.
- `tests/tsqlt/` — Reserved for future tSQLt regression suites.
