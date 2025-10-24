---
description: "Scaffold tSQLt tests for staging outputs"
mode: 'agent'
---

Tasks:
1) If tSQLt not present, add `tests/tsqlt/install_tsqlt.sql` (download instructions + install script).
2) Create test class schemas (e.g., `tests_Admit`, `tests_Billing`, …).
3) For each staged view/table, create tests:
   - Type assertions (DECIMAL(19,2) for money, DATE/DATETIME2 for dates).
   - Domain checks (gender set, insurance types mapped).
   - Math tie-outs (total = sum of components; patient_due = total - insurance).
   - Date logic (admission/discharge/LOS rules).
4) Add `tests/tsqlt/run_all.sql` to run everything and return an XML + text summary.

Apply edits to files; no long SQL in chat.
