---
description: "Create timing harness that avoids full scans and logs to ops.benchmarks"
mode: 'agent'
---

Create `ops/benchmarking/timing_runner.sql` that:
- Samples each *_std_vw with: `TOP (100)`, a narrow filtered query (current month by date col), and a lightweight aggregate.
- Captures CPU, elapsed_ms, logical_reads using Query Store sys views if available; else fall back to `SET STATISTICS TIME, IO ON` plus `sys.dm_exec_query_stats`.
- Inserts one row per probe into `[ops].[benchmarks]`.
- Provide a runnable Agent job script `ops/benchmarking/agent_job_timing.sql`.

Rules:
- No TRY_PARSE/FORMAT.
- Create `[ops].[benchmarks]` if missing.
- Apply file edits; no results in chat.
