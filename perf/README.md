# Staging View Performance Workbench

This folder contains artifacts created during the staging-view performance triage.

- `diagnostics.sql` – repeatable harness to capture actual execution plans, STATISTICS IO/TIME output, and DMV snapshots around the representative queries.
- `baseline.md` – baseline timing/IO summaries for the current (unoptimized) staging views.
- `findings.md` – catalog of the key anti-patterns, evidence, and recommended changes.
- `rewrites/` – targeted rewrite drafts for each view, incorporating the recommended fixes without altering business semantics.
- `index_recommendations.sql` – nonclustered index proposals derived from observed predicates and filters.
- `stats_maintenance.sql` – guidance for keeping statistics and database options in sync with the workload.
- `staging_decision.md` – memo weighing view-only staging against materialized tables and indexed views.

Use `diagnostics.sql` to build new baselines after applying the rewrites. Update `baseline.md` with the fresh metrics so regressions are captured over time.
