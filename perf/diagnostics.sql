USE [Healthcare];
GO

/*
    Diagnostics harness for staging views.
    Run in SSMS with "Include Actual Execution Plan" enabled.
    Captures:
      - STATISTICS IO/TIME output
      - Key DMV snapshots before/after
      - Representative queries per view
*/

SET NOCOUNT ON;

------------------------------------------------------------
-- Session options to capture runtime and plan detail
------------------------------------------------------------
SET STATISTICS IO, TIME ON;
SET STATISTICS XML ON;

------------------------------------------------------------
-- Snapshot DMVs before workload
------------------------------------------------------------
SELECT SYSDATETIME() AS capture_time, 'pre-run' AS phase;

SELECT *
FROM sys.dm_exec_requests
WHERE session_id <> @@SPID;

SELECT *
FROM sys.dm_tran_locks
WHERE request_session_id <> @@SPID;

SELECT *
FROM sys.dm_os_waiting_tasks
WHERE session_id <> @@SPID;

SELECT TOP (20)
    qs.total_elapsed_time / NULLIF(qs.execution_count, 0) AS avg_elapsed_microseconds,
    qs.total_worker_time / NULLIF(qs.execution_count, 0) AS avg_cpu_microseconds,
    DB_NAME(st.dbid) AS database_name,
    OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS schema_name,
    OBJECT_NAME(st.objectid, st.dbid) AS object_name,
    st.text
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE st.dbid = DB_ID('Healthcare')
ORDER BY qs.total_elapsed_time DESC;

------------------------------------------------------------
-- Representative queries (enable actual plan capture in SSMS)
------------------------------------------------------------

-- patients_std
SELECT TOP (30)
    *
FROM stg.patients_std;

SELECT *
FROM stg.patients_std
WHERE patient_id = 1001;

-- admissions_std
SELECT TOP (30)
    *
FROM stg.admissions_std;

SELECT *
FROM stg.admissions_std
WHERE admission_id = 1001;

-- billing_std
SELECT TOP (30)
    *
FROM stg.billing_std;

SELECT *
FROM stg.billing_std
WHERE payment_status = 'Paid';

-- diagnoses_std
SELECT TOP (30)
    *
FROM stg.diagnoses_std;

SELECT *
FROM stg.diagnoses_std
WHERE icd_code = 'I10';

-- procedures_std
SELECT TOP (30)
    *
FROM stg.procedures_std;

SELECT *
FROM stg.procedures_std
WHERE cpt_code = '99213';

-- lab_results_std
SELECT TOP (30)
    *
FROM stg.lab_results_std;

SELECT *
FROM stg.lab_results_std
WHERE test_name = 'Hemoglobin';

------------------------------------------------------------
-- Snapshot DMVs after workload
------------------------------------------------------------

SELECT SYSDATETIME() AS capture_time, 'post-run' AS phase;

SELECT *
FROM sys.dm_exec_requests
WHERE session_id <> @@SPID;

SELECT *
FROM sys.dm_tran_locks
WHERE request_session_id <> @@SPID;

SELECT *
FROM sys.dm_os_waiting_tasks
WHERE session_id <> @@SPID;

SELECT TOP (20)
    qs.total_elapsed_time / NULLIF(qs.execution_count, 0) AS avg_elapsed_microseconds,
    qs.total_worker_time / NULLIF(qs.execution_count, 0) AS avg_cpu_microseconds,
    DB_NAME(st.dbid) AS database_name,
    OBJECT_SCHEMA_NAME(st.objectid, st.dbid) AS schema_name,
    OBJECT_NAME(st.objectid, st.dbid) AS object_name,
    st.text
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE st.dbid = DB_ID('Healthcare')
ORDER BY qs.total_elapsed_time DESC;

------------------------------------------------------------
-- Reset
------------------------------------------------------------
SET STATISTICS XML OFF;
SET STATISTICS IO, TIME OFF;
