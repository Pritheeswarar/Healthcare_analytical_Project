# Healthcare SQL Analytics

## Quickstart Checklist

- Clone the repository into your secure analytics workspace.
- Install the VS Code MSSQL extension and SQLFluff (see `standards/`).
- Configure an environment-based connection profile (no secrets committed).
- Review runbook steps below and execute a sample KPI query.
- Validate coding standards before opening your first PR.

## Purpose

This project provides reusable SQL assets to support core hospital analytics use cases:

- Track hospital throughput (arrival-to-discharge flow), length of stay, and unit occupancy.
- Monitor revenue per admission and payer mix trends across lines of service.
- Surface leading CPT and ICD codes to inform coding accuracy and reimbursement.
- Flag 30-day readmissions for quality improvement programs.
- Measure lab order completion rates and turnaround times for operational readiness.

## Repository Layout

```text
sql/
  00_metadata/        -- Source system dictionaries, table inventories, lineage
  01_staging/         -- Raw-to-staging extracts, minimal transformations
  02_transformations/ -- Business logic, normalization, surrogate keys
  03_kpi/             -- Final KPI queries, dashboards, export-ready SQL
  04_tests/           -- Data quality checks, regression tests, harness SQL

docs/                 -- Governance notes, process docs, analyst guides
standards/            -- SQL style, naming conventions, reusable templates
```

## Secure Connections with VS Code MSSQL Extension

1. Install the **SQL Server (mssql)** extension and reload VS Code.
2. Create a `.env` (not committed) or use the system Keychain/Windows Credential Manager to store:
   - `MSSQL_SERVER`, `MSSQL_DB`, `MSSQL_USER` (if using SQL auth).
3. In the VS Code Command Palette (`Ctrl+Shift+P`) run `MS SQL: Create Connection Profile` and select **Save Password?** → `No`.
4. When prompted for the password, paste it from a temporary environment variable using the secure input box.
5. On reconnect, allow the profile to prompt for a password each session. If you need an ephemeral plain-text copy, use the integrated terminal:

   ```powershell
   $secure = Read-Host "Enter MSSQL Password" -AsSecureString
   $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
   $env:MSSQL_PWD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
   [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
   ```

   Paste `$env:MSSQL_PWD` into the MSSQL password prompt and avoid saving it in the profile.
6. Clear the temporary variable immediately: `Remove-Item Env:MSSQL_PWD`.

## Runbook: Executing KPIs and Exporting Results

1. Open the desired query in `sql/03_kpi/` within VS Code.
2. Ensure the SQL document adheres to the standards in `standards/` and references only governed objects.
3. Press `Ctrl+Shift+E` (or use the Command Palette) to run the query against the active connection.
4. Review the Results pane:
   - Use **Run Current Statement** for iterative testing.
   - Confirm row counts and spot-check aggregates.
5. Export to CSV:
   - Click the disk icon in the Results grid and choose **Save as CSV...**.
   - Save outputs in a secure, time-stamped location (outside the repo) and document consumers in the PR notes.
6. Log extraction details (dataset name, filters, timestamp) in your analytics ticket to maintain lineage.

## Pull Request Expectations

- **Linting:** Run `sqlfluff lint sql` (configure SQLFluff in `pyproject.toml` or `.sqlfluff`). Resolve violations before submitting.
- **Testing:** Add or update regression coverage in `sql/04_tests/` and note how to execute the checks.
- **Data Caveats:** Summarize population filters, known data delays, suppressed cohorts, and any manual adjustments in the PR description.
- **Documentation:** Reference relevant runbook sections and update `docs/` if operational steps change.

## SQLFluff Workflow

- Install SQLFluff (`pip install sqlfluff`) and confirm the repo picks up `.sqlfluff` automatically.
- Run `sqlfluff lint sql` to review violations; use `sqlfluff lint path/to/file.sql` for targeted checks.
- Apply safe auto-fixes with `sqlfluff fix sql --force` before opening a PR, then re-run lint to ensure a clean pass.

## Additional Resources

- `docs/data-governance.md` for HIPAA de-identification guidance.
- `standards/naming-conventions.md` and `standards/query-style.md` for authoring expectations.
