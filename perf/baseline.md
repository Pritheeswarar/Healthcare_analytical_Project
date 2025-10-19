# Baseline Measurements (Current Views)

Environment: SQL Server 16.0 (on-prem), database `Healthcare`.

> Baseline timings not yet collected. Run `perf/diagnostics.sql` with STATISTICS IO/TIME enabled and record the duration and IO figures below.

| View | Query | Duration | CPU | Logical Reads | Notes |
|------|-------|----------|-----|---------------|-------|
| `stg.patients_std` | `SELECT TOP (30) * ...` | 1030.53 s | 1009.59 s | 2 | Most time inside CROSS APPLY expansion; base table reads stay low while worktables churn. |
| `stg.patients_std` | `SELECT * WHERE patient_id = 1001` | 0.62 s | 0.61 s | 3525 | Single patient lookup still forces whole CROSS APPLY chain. |
| `stg.admissions_std` | `SELECT TOP (30) * ...` | 4.33 s | 3.99 s | 11511 | Heavy scan across admissions source. |
| `stg.admissions_std` | `SELECT * WHERE admission_id = 1001` | 3.22 s | 3.08 s | 11511 | Filter remains non-sargable; expands admissions source. |
| `stg.billing_std` | `SELECT TOP (30) * ...` | 0.17 s | 0.02 s | 1 | Mostly metadata touch; view still expands but finishes quickly. |
| `stg.billing_std` | `SELECT * WHERE payment_status = 'Paid'` | 108.15 s | 102.84 s | 11349 | Predicate forces full expansion over billing source. |
| `stg.diagnoses_std` | `SELECT TOP (30) * ...` | 0.39 s | 0.23 s | 1 | Similar pattern to billing top sample. |
| `stg.diagnoses_std` | `SELECT * WHERE icd_code = 'I10'` | 81.26 s | 79.42 s | 15204 | Shows worst-case expansion; no rows returned but all logic executed. |
| `stg.procedures_std` | `SELECT TOP (30) * ...` | 20.07 s | 19.20 s | 3273 | CROSS APPLY parsing and string routines dominate. |
| `stg.procedures_std` | `SELECT * WHERE cpt_code = '99213'` | 19.83 s | 18.31 s | 3273 | Heavy TRY_PARSE and string work per row. |
| `stg.lab_results_std` | `SELECT TOP (30) * ...` | 0.22 s | 0.03 s | 1 | Lightweight sample. |
| `stg.lab_results_std` | `SELECT * WHERE test_name = 'Hemoglobin'` | 41.47 s | 37.27 s | 38627 | Filter drives expensive expansion across lab results. |

Update this table after the diagnostics run.
