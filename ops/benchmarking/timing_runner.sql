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
        plan_id uniqueidentifier NOT NULL CONSTRAINT DF_benchmarks_plan DEFAULT (NEWID())
    );
END;
ELSE
BEGIN
    IF EXISTS
    (
        SELECT 1
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(N'ops.benchmarks')
            AND c.name = N'plan_id'
            AND c.is_nullable = 1
    )
    BEGIN
        UPDATE ops.benchmarks
        SET plan_id = ISNULL(plan_id, NEWID())
        WHERE plan_id IS NULL;

        ALTER TABLE ops.benchmarks
            ALTER COLUMN plan_id uniqueidentifier NOT NULL;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.default_constraints AS dc
        WHERE dc.parent_object_id = OBJECT_ID(N'ops.benchmarks')
            AND dc.parent_column_id = COLUMNPROPERTY(OBJECT_ID(N'ops.benchmarks'), N'plan_id', N'ColumnId')
    )
    BEGIN
        ALTER TABLE ops.benchmarks
            ADD CONSTRAINT DF_benchmarks_plan DEFAULT (NEWID()) FOR plan_id;
    END;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ops.benchmarks')
        AND name = N'IX_benchmarks_object_metric_run'
)
BEGIN
    BEGIN TRY
        CREATE NONCLUSTERED INDEX IX_benchmarks_object_metric_run
            ON ops.benchmarks (object_name, metric, run_utc);
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() <> 1913
            THROW;
    END CATCH;
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

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

DECLARE @script_name sysname = N'timing_runner';
DECLARE @counterpart_name sysname = N'staging_healthcheck';
DECLARE @plan_origin sysname = TRY_CONVERT(sysname, SESSION_CONTEXT(N'ops_plan_origin'));
DECLARE @plan_id UNIQUEIDENTIFIER = TRY_CONVERT(uniqueidentifier, SESSION_CONTEXT(N'ops_plan_id'));
DECLARE @reset_plan_after_run bit = 0;

IF @plan_id IS NULL
   OR @plan_origin IS NULL
   OR @plan_origin = @script_name
BEGIN
    SET @plan_id = NEWID();
    EXEC sys.sp_set_session_context @key = N'ops_plan_id', @value = @plan_id, @read_only = 0;
    EXEC sys.sp_set_session_context @key = N'ops_plan_origin', @value = @script_name, @read_only = 0;
END
ELSE IF @plan_origin = @counterpart_name
BEGIN
    SET @reset_plan_after_run = 1;
END
ELSE
BEGIN
    SET @plan_id = NEWID();
    EXEC sys.sp_set_session_context @key = N'ops_plan_id', @value = @plan_id, @read_only = 0;
    EXEC sys.sp_set_session_context @key = N'ops_plan_origin', @value = @script_name, @read_only = 0;
END;

PRINT 'plan_id = ' + CONVERT(nvarchar(36), @plan_id);

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
    ('stg_optimized.patients_std', 'stg_optimized', 'patients_std'),
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
    value sql_variant NOT NULL
);

DECLARE @max_ordinal int = (SELECT MAX(ordinal) FROM @objects);
DECLARE @ordinal int = 1;
DECLARE @rows_inserted int = 0;

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
    DECLARE @probe_sql nvarchar(max) = N'SELECT 1 ' + @comment + N' WHERE EXISTS (SELECT 1 FROM '
                                     + @qualified_name
                                     + N' WHERE 1 = 0);';
    DECLARE @qs_like_plain nvarchar(400) = N'%FROM ' + @object_name + N'%';
    DECLARE @qs_like_bracket nvarchar(400) = N'%FROM [' + @schema_name + N'].[' + @object_only + N']%';
    DECLARE @probe_start datetime2(7) = SYSUTCDATETIME();
    EXEC sp_executesql @probe_sql;
    DECLARE @fallback_elapsed_ms int = DATEDIFF(MILLISECOND, @probe_start, SYSUTCDATETIME());

    IF @query_store_on = 1
        EXEC sys.sp_query_store_flush_db;

    DECLARE @qs_sample_cpu_ms bigint = NULL;
    DECLARE @qs_sample_elapsed_ms bigint = NULL;
    DECLARE @qs_sample_reads bigint = NULL;
    DECLARE @qs_avg_cpu_ms bigint = NULL;
    DECLARE @qs_avg_elapsed_ms bigint = NULL;
    DECLARE @qs_avg_reads bigint = NULL;
    DECLARE @qs_exec_count bigint = NULL;

    IF @query_store_on = 1
    BEGIN
        SELECT TOP (1)
            @qs_sample_cpu_ms = CONVERT(bigint, rs.last_cpu_time / 1000),
            @qs_sample_elapsed_ms = CONVERT(bigint, rs.last_duration / 1000),
            @qs_sample_reads = rs.last_logical_io_reads,
            @qs_avg_cpu_ms = CONVERT(bigint, rs.avg_cpu_time / 1000),
            @qs_avg_elapsed_ms = CONVERT(bigint, rs.avg_duration / 1000),
            @qs_avg_reads = rs.avg_logical_io_reads,
            @qs_exec_count = rs.count_executions
        FROM sys.query_store_query_text AS qt
            INNER JOIN sys.query_store_query AS q
                ON q.query_text_id = qt.query_text_id
            INNER JOIN sys.query_store_plan AS p
                ON p.query_id = q.query_id
            INNER JOIN sys.query_store_runtime_stats AS rs
                ON rs.plan_id = p.plan_id
        WHERE qt.query_sql_text LIKE '%' + @comment + '%'
            OR qt.query_sql_text LIKE @qs_like_plain
            OR qt.query_sql_text LIKE @qs_like_bracket
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

    INSERT INTO @results (object_name, metric, value)
    VALUES
        (@object_name, 'timing:sample_recent', CAST(@sample_value AS sql_variant));

    IF @qs_avg_cpu_ms IS NOT NULL OR @qs_avg_elapsed_ms IS NOT NULL OR @qs_avg_reads IS NOT NULL
    BEGIN
        DECLARE @aggregate_value nvarchar(200) = CONCAT(
                                            'cpu_ms=',
                                            ISNULL(CONVERT(varchar(30), @qs_avg_cpu_ms), 'NULL'),
                                            '|elapsed_ms=',
                                            ISNULL(CONVERT(varchar(30), @qs_avg_elapsed_ms), 'NULL'),
                                            '|logical_reads=',
                                            ISNULL(CONVERT(varchar(30), @qs_avg_reads), 'NULL'),
                                            '|exec_count=',
                                            ISNULL(CONVERT(varchar(30), @qs_exec_count), '0')
                                        );

        INSERT INTO @results (object_name, metric, value)
        VALUES
            (@object_name, 'timing:recent_aggregate', CAST(@aggregate_value AS sql_variant));
    END;

    SET @ordinal += 1;
END;

DECLARE @insert_utc datetime2(7) = SYSUTCDATETIME();

INSERT INTO ops.benchmarks (object_name, metric, value, run_utc, plan_id)
SELECT object_name, metric, value, @insert_utc, @plan_id
FROM @results;

SET @rows_inserted = @@ROWCOUNT;

PRINT 'timing_runner inserted ' + CONVERT(varchar(12), @rows_inserted)
      + ' rows for plan_id ' + CONVERT(nvarchar(36), @plan_id);
PRINT 'Timing runner plan_id = ' + @token;

SELECT
    plan_id = @plan_id,
    object_name,
    metric,
    value,
    run_utc
FROM ops.benchmarks
WHERE plan_id = @plan_id
ORDER BY object_name, metric;

IF @reset_plan_after_run = 1
BEGIN
    EXEC sys.sp_set_session_context @key = N'ops_plan_id', @value = NULL, @read_only = 0;
    EXEC sys.sp_set_session_context @key = N'ops_plan_origin', @value = NULL, @read_only = 0;
END;
