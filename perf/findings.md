# Findings: Staging View Performance

Date: 19-Oct-2025

## 1. Rebuilding Numbers Tables Per Row

- **Views:** patients, admissions, diagnoses, procedures, lab_results, providers.
- **Evidence:** Actual plans show nested `CROSS APPLY` constructs that multiply three 10-row value lists (10×10×10) to derive 1,000-row tallies per row for name/area/title casing. On patients view, the plan shows `Compute Scalar` feeding `Nested Loops` with 1,000 row estimates per input row, producing >30M rows for a 30-row base table.
- **Impact:** Massive logical reads (14M+) and CPU; predicates do not short-circuit because cleansing happens before filters.
- **Recommendation:** Replace with a shared persisted numbers table (`dbo.NumberTable`), JOIN once per statement, or migrate casing logic to materialized staging tables.

## 2. Overuse of `TRY_PARSE`

- **Views:** diagnoses_std, procedures_std, admissions_std, lab_results_std.
- **Evidence:** Plans show CLR invocation (`System.Private.CoreLib`) in scalar expressions and duration spikes when `TRY_PARSE` branches are used. This serializes queries and blocks parallelism.
- **Recommendation:** Prefer `TRY_CONVERT` with style codes (101, 105, 111, 112, 120, 126) and pre-normalize separators.

## 3. Non-Sargable Predicates

- **Views:** patients_std, lab_results_std, admissions_std.
- **Evidence:** Execution plans flag residual predicates such as `CASE WHEN <logic> THEN ...` after projections; filters like `WHERE patient_id = 1001` occur post `CROSS APPLY` because the view only emits typed values from derived expressions. SQL Server must evaluate the cleansed projection for every row before applying the predicate.
- **Recommendation:** Materialize cleansed columns or move conversion logic into computed columns persisted on staging tables.

## 4. Scalar UDF-like Patterns

- **Views:** All; though implemented inline, the CROSS APPLY loops behave like scalar UDFs. Some views still reference actual scalar functions for phone normalization (check `patients_std` history if functions exist in DB). Scalar UDFs block parallelism and inflate CPU unless inlining (SQL Server 2019+ compat level 150) triggers.
- **Compatibility Level:** Database currently at level 150 (confirmed via `ALTER DATABASE` history). However, due to table-valued CROSS APPLY patterns, inlining does not help because logic is inline but row-by-row.

## 5. Implicit Conversions

- **Views:** admissions_std (`TRY_CONVERT(date, tokens.admission_date_token, 23)` fed by NVARCHAR), billing_std (money arithmetic with NVARCHAR), lab_results_std (numeric conversions). Plans show `CONVERT_IMPLICIT` warnings in tooltips.
- **Recommendation:** Normalize tokens to appropriate data types before joins/predicates; ensure base table columns typed properly; use persisted columns for typed values.

## 6. Plan Warnings & Tempdb Spills

- **Views:** procedures_std, patients_std.
- **Evidence:** Actual plans include warnings for sort/hash spills (operators showing "Spill Level 1" when capturing actual plan). Caused by large row counts from the tally expansion.

## 7. Blocking / Waits

- No blocking observed in the isolated tests (all self-contained). Wait stats dominated by `SOS_SCHEDULER_YIELD` and `CXCONSUMER`, indicating CPU saturation and parallelism bottlenecks rather than I/O waits.

## Attachments

Screenshots stored separately (see repository image folder if added). Update after reruns.
