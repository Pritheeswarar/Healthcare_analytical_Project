-- Note: Benchmarks stg_optimized workloads; transform/KPI timing harnesses will be added separately.
USE [Healthcare];

IF SCHEMA_ID('ops') IS NULL
    EXEC('CREATE SCHEMA ops');

IF OBJECT_ID('ops.benchmarks', 'U') IS NULL
BEGIN
    CREATE TABLE ops.benchmarks
    (
        benchmark_id bigint IDENTITY(1, 1) PRIMARY KEY,
        object_name sysname NOT NULL,
        metric nvarchar(200) NOT NULL,
        value sql_variant NOT NULL,
        run_utc datetime2(7) NOT NULL DEFAULT (SYSUTCDATETIME()),
        plan_id uniqueidentifier NULL
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('ops.benchmarks')
        AND name = 'IX_benchmarks_recent'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_benchmarks_recent
        ON ops.benchmarks (run_utc DESC, object_name, metric);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.database_query_store_options
    WHERE actual_state_desc IN ('READ_ONLY', 'READ_WRITE')
)
BEGIN
    ALTER DATABASE CURRENT SET QUERY_STORE = ON;
    ALTER DATABASE CURRENT SET QUERY_STORE
    (
        OPERATION_MODE = READ_WRITE,
        QUERY_CAPTURE_MODE = AUTO,
        INTERVAL_LENGTH_MINUTES = 10
    );
END;

DECLARE @query_store_on bit = CASE
                                  WHEN EXISTS
                                  (
                                      SELECT 1
                                      FROM sys.database_query_store_options
                                      WHERE actual_state_desc IN ('READ_ONLY', 'READ_WRITE')
                                  )
                                  THEN 1
                                  ELSE 0
                              END;

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @plan_id uniqueidentifier = NEWID();
DECLARE @token nvarchar(40) = CONVERT(nvarchar(36), @plan_id);

DECLARE @objects TABLE
(
    ordinal int IDENTITY(1, 1) PRIMARY KEY,
    object_name sysname NOT NULL,
    schema_name sysname NOT NULL,
    object_only sysname NOT NULL
);

INSERT INTO @objects (object_name, schema_name, object_only)
VALUES
    ('stg_optimized.admissions_std', 'stg_optimized', 'admissions_std'),
    ('stg_optimized.billing_std', 'stg_optimized', 'billing_std'),
    ('stg_optimized.diagnoses_std', 'stg_optimized', 'diagnoses_std'),
    ('stg_optimized.procedures_std', 'stg_optimized', 'procedures_std'),
    ('stg_optimized.lab_results_std', 'stg_optimized', 'lab_results_std'),
    ('stg_optimized.providers_std', 'stg_optimized', 'providers_std'),
    ('stg_optimized.departments_std', 'stg_optimized', 'departments_std'),
    ('stg_optimized.hospitals_std', 'stg_optimized', 'hospitals_std');

DECLARE @results TABLE
(
    object_name sysname NOT NULL,
    metric nvarchar(200) NOT NULL,
    value sql_variant NOT NULL,
    plan_id uniqueidentifier NOT NULL
);

DECLARE @max_ordinal int = (SELECT MAX(ordinal) FROM @objects);
DECLARE @ordinal int = 1;

WHILE @ordinal <= @max_ordinal
BEGIN
    DECLARE @object_name sysname;
    DECLARE @schema_name sysname;
    DECLARE @object_only sysname;

    SELECT
        @object_name = object_name,
        @schema_name = schema_name,
        @object_only = object_only
    FROM @objects
    WHERE ordinal = @ordinal;

    DECLARE @qualified_name nvarchar(514) = QUOTENAME(@schema_name) + N'.' + QUOTENAME(@object_only);
    DECLARE @comment nvarchar(200) = N'/*bench:' + @token + N';obj:' + @object_name + N'*/';
    DECLARE @sql nvarchar(max) = N'SELECT COUNT_BIG(1) ' + @comment + N' FROM ' + @qualified_name + N' WITH (NOLOCK) OPTION (MAXDOP 1);';

    EXEC sp_executesql @sql;

    DECLARE @fallback_elapsed_ms int;
    DECLARE @probe_start datetime2(7) = SYSUTCDATETIME();
    EXEC sp_executesql @sql;
    SET @fallback_elapsed_ms = DATEDIFF(MILLISECOND, @probe_start, SYSUTCDATETIME());

    IF @query_store_on = 1
        EXEC sys.sp_query_store_flush_db;

    DECLARE @qs_sample_cpu_ms decimal(18, 2) = NULL;
    DECLARE @qs_sample_elapsed_ms decimal(18, 2) = NULL;
    DECLARE @qs_sample_reads bigint = NULL;
    DECLARE @qs_avg_cpu_ms decimal(18, 2) = NULL;
    DECLARE @qs_avg_elapsed_ms decimal(18, 2) = NULL;
    DECLARE @qs_avg_reads decimal(18, 2) = NULL;
    DECLARE @qs_exec_count bigint = NULL;

    IF @query_store_on = 1
    BEGIN
        SELECT TOP (1)
            @qs_sample_cpu_ms = CAST(rs.last_cpu_time / 1000.0 AS decimal(18, 2)),
            @qs_sample_elapsed_ms = CAST(rs.last_duration / 1000.0 AS decimal(18, 2)),
            @qs_sample_reads = rs.last_logical_io_reads,
            @qs_avg_cpu_ms = CAST(rs.avg_cpu_time / 1000.0 AS decimal(18, 2)),
            @qs_avg_elapsed_ms = CAST(rs.avg_duration / 1000.0 AS decimal(18, 2)),
            @qs_avg_reads = CAST(rs.avg_logical_io_reads AS decimal(18, 2)),
            @qs_exec_count = rs.count_executions
        FROM sys.query_store_query_text AS qt
            INNER JOIN sys.query_store_query AS q
                ON q.query_text_id = qt.query_text_id
            INNER JOIN sys.query_store_plan AS p
                ON p.query_id = q.query_id
            INNER JOIN sys.query_store_runtime_stats AS rs
                ON rs.plan_id = p.plan_id
        WHERE qt.query_sql_text LIKE '%' + @comment + '%'
        ORDER BY rs.last_execution_time DESC;
    END;

    DECLARE @sample_value nvarchar(200);

    IF @qs_sample_cpu_ms IS NOT NULL OR @qs_sample_elapsed_ms IS NOT NULL OR @qs_sample_reads IS NOT NULL
    BEGIN
        SET @sample_value = CONCAT(
                            'cpu_ms=',
                            ISNULL(CONVERT(varchar(30), @qs_sample_cpu_ms), 'NULL'),
                            '|elapsed_ms=',
                            ISNULL(CONVERT(varchar(30), @qs_sample_elapsed_ms), 'NULL'),
                            '|logical_reads=',
                            ISNULL(CONVERT(varchar(30), @qs_sample_reads), 'NULL')
                        );
    END
    ELSE
    BEGIN
        SET @sample_value = CONCAT(
                            'cpu_ms=NULL|elapsed_ms=',
                            CONVERT(varchar(30), @fallback_elapsed_ms),
                            '|logical_reads=NULL'
                        );
    END;

    INSERT INTO @results (object_name, metric, value, plan_id)
    VALUES
        (@object_name, 'timing:sample_recent', CAST(@sample_value AS sql_variant), @plan_id);

    IF @qs_avg_cpu_ms IS NOT NULL OR @qs_avg_elapsed_ms IS NOT NULL OR @qs_avg_reads IS NOT NULL
    BEGIN
        DECLARE @aggregate_value nvarchar(200) = CONCAT(
                                            'avg_cpu_ms=',
                                            ISNULL(CONVERT(varchar(30), @qs_avg_cpu_ms), 'NULL'),
                                            '|avg_elapsed_ms=',
                                            ISNULL(CONVERT(varchar(30), @qs_avg_elapsed_ms), 'NULL'),
                                            '|avg_logical_reads=',
                                            ISNULL(CONVERT(varchar(30), @qs_avg_reads), 'NULL'),
                                            '|exec_count=',
                                            ISNULL(CONVERT(varchar(30), @qs_exec_count), '0')
                                        );

        INSERT INTO @results (object_name, metric, value, plan_id)
        VALUES
            (@object_name, 'timing:recent_aggregate', CAST(@aggregate_value AS sql_variant), @plan_id);
    END;

    SET @ordinal += 1;
END;

INSERT INTO ops.benchmarks (object_name, metric, value, plan_id)
SELECT object_name, metric, value, plan_id
FROM @results;

SELECT 'Timing runner plan_id = ' + @token AS msg;
