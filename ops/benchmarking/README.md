# Staging Health Check Benchmark

This package provides a repeatable benchmark sweep over the `stg_optimized` staging views so that data types, placeholder handling, and date ranges can be sanity-checked and timed with lightweight samples.

## How to run

1. Open `ops/benchmarking/staging_healthcheck.sql` in SQL Server Management Studio or Azure Data Studio.
2. Execute the entire script (F5). The script:
   - Ensures the `[ops].[benchmarks]` logging table exists.
   - Captures a `plan_id` for this execution and writes row counts plus targeted aggregates for each staging view.
   - Returns the first 100 rows for each view in clustered-key order for visual spot checks.
   - Emits `SELECT * FROM [ops].[benchmarks]` restricted to the past hour so the newest run is visible at the bottom of the grid.

The script runs with `READ UNCOMMITTED` to avoid blocking production workloads and relies only on `TRY_CONVERT` casting, so it can be scheduled in Agent without additional permissions.

## Exporting results

- **Benchmarks table:** Right-click the final result grid and choose *Save Results As* to produce CSV, or add `FOR JSON PATH` to the final `SELECT` when you need JSON output. Because all metrics share a `plan_id`, you can export a single run with `SELECT * FROM ops.benchmarks WHERE plan_id = '<plan_id>'`.
- **Sample rows:** Each `SELECT TOP (100)` result set can be saved from the grid or piped to CSV via `sqlcmd`:

  ```powershell
  sqlcmd -S <server> -d Healthcare -i ops\benchmarking\staging_healthcheck.sql -o ops\benchmarking\out\staging_healthcheck.csv -s "," -W
  ```

  Adjust the output path or delimiter as needed for downstream review folders.

## Next steps

- Schedule the script via SQL Agent (daily/weekly) to populate a time series of staging health metrics.
- Layer simple alerts by watching `[ops].[benchmarks]` for metric deviations (for example, rowcount drops or new null-count spikes).
- Feed the latest CSV exports into documentation or QA sign-offs before promoting downstream transformations.
