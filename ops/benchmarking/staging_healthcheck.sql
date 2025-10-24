-- Note: Targets stg_optimized sources today; downstream transform/KPI health checks will follow in separate scripts.
USE [Healthcare];
GO

IF SCHEMA_ID('ops') IS NULL
    EXEC('CREATE SCHEMA ops');
GO

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
GO

IF OBJECT_ID('ops.benchmarks', 'U') IS NOT NULL
   AND NOT EXISTS
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
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @script_name sysname = N'staging_healthcheck';
DECLARE @counterpart_name sysname = N'timing_runner';
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

DECLARE @utc_now datetime2(7) = SYSUTCDATETIME();
DECLARE @start_30 date = DATEADD(DAY, -30, CAST(@utc_now AS date));
DECLARE @start_90 date = DATEADD(DAY, -90, CAST(@utc_now AS date));
DECLARE @rows_inserted int = 0;

BEGIN TRY
    BEGIN TRAN;

    DECLARE @results TABLE
    (
        object_name sysname NOT NULL,
        metric nvarchar(200) NOT NULL,
        value sql_variant NOT NULL
    );

    WITH patients AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            rows_last_30d = SUM(
                                   CASE
                                       WHEN TRY_CONVERT(date, created_date) >= @start_30
                                       THEN 1
                                       ELSE 0
                                   END
                               ),
            null_gender_last_30d = SUM(
                                           CASE
                                               WHEN TRY_CONVERT(date, created_date) >= @start_30
                                                    AND (gender IS NULL OR gender IN ('Unknown', 'NA'))
                                               THEN 1
                                               ELSE 0
                                           END
                                       )
        FROM stg_optimized.patients_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.patients_std',
        m.metric,
        m.value
    FROM patients AS p
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(p.rowcount_metadata AS sql_variant)),
            ('rows_last_30d', CAST(COALESCE(p.rows_last_30d, 0) AS sql_variant)),
            ('null_gender_last_30d', CAST(COALESCE(p.null_gender_last_30d, 0) AS sql_variant))
    ) AS m(metric, value);

    WITH admissions AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            admissions_last_30d = SUM(CASE WHEN TRY_CONVERT(date, admission_date) >= @start_30 THEN 1 ELSE 0 END),
            los_sum_last_30d = SUM(
                                       CASE
                                           WHEN TRY_CONVERT(date, admission_date) >= @start_30
                                           THEN COALESCE(TRY_CONVERT(decimal(18, 4), length_of_stay), 0)
                                           ELSE 0
                                       END
                                   ),
            los_count_last_30d = SUM(
                                         CASE
                                             WHEN TRY_CONVERT(date, admission_date) >= @start_30
                                                  AND TRY_CONVERT(decimal(18, 4), length_of_stay) IS NOT NULL
                                             THEN 1
                                             ELSE 0
                                         END
                                     )
        FROM stg_optimized.admissions_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.admissions_std',
        m.metric,
        m.value
    FROM admissions AS a
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(a.rowcount_metadata AS sql_variant)),
            ('admissions_last_30d', CAST(COALESCE(a.admissions_last_30d, 0) AS sql_variant)),
            (
                'avg_los_last_30d',
                CAST(
                    CASE
                        WHEN COALESCE(a.los_count_last_30d, 0) > 0
                        THEN COALESCE(a.los_sum_last_30d, 0)
                             / CONVERT(decimal(18, 4), a.los_count_last_30d)
                        ELSE CONVERT(decimal(18, 4), 0)
                    END AS sql_variant
                )
            )
    ) AS m(metric, value);

    WITH billing AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            total_amount_last_30d = SUM(
                                             CASE
                                                 WHEN TRY_CONVERT(date, bill_date) >= @start_30
                                                 THEN COALESCE(TRY_CONVERT(decimal(19, 4), total_amount), 0)
                                                 ELSE 0
                                             END
                                         ),
            patient_due_last_30d = SUM(
                                            CASE
                                                WHEN TRY_CONVERT(date, bill_date) >= @start_30
                                                THEN COALESCE(TRY_CONVERT(decimal(19, 4), patient_due), 0)
                                                ELSE 0
                                            END
                                        )
        FROM stg_optimized.billing_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.billing_std',
        m.metric,
        m.value
    FROM billing AS b
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(b.rowcount_metadata AS sql_variant)),
            ('total_amount_last_30d', CAST(COALESCE(b.total_amount_last_30d, CONVERT(decimal(19, 4), 0)) AS sql_variant)),
            ('patient_due_last_30d', CAST(COALESCE(b.patient_due_last_30d, CONVERT(decimal(19, 4), 0)) AS sql_variant))
    ) AS m(metric, value);

    WITH diagnoses AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            diagnoses_last_90d = SUM(CASE WHEN TRY_CONVERT(date, diagnosis_date) >= @start_90 THEN 1 ELSE 0 END),
            null_icd_last_90d = SUM(
                                     CASE
                                         WHEN TRY_CONVERT(date, diagnosis_date) >= @start_90 AND icd_code IS NULL
                                         THEN 1
                                         ELSE 0
                                     END
                                 )
        FROM stg_optimized.diagnoses_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.diagnoses_std',
        m.metric,
        m.value
    FROM diagnoses AS d
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(d.rowcount_metadata AS sql_variant)),
            ('diagnoses_last_90d', CAST(COALESCE(d.diagnoses_last_90d, 0) AS sql_variant)),
            ('null_icd_last_90d', CAST(COALESCE(d.null_icd_last_90d, 0) AS sql_variant))
    ) AS m(metric, value);

    WITH procedures AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            procedures_last_90d = SUM(CASE WHEN TRY_CONVERT(date, procedure_date) >= @start_90 THEN 1 ELSE 0 END),
            null_cpt_last_90d = SUM(
                                     CASE
                                         WHEN TRY_CONVERT(date, procedure_date) >= @start_90 AND cpt_code IS NULL
                                         THEN 1
                                         ELSE 0
                                     END
                                 )
        FROM stg_optimized.procedures_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.procedures_std',
        m.metric,
        m.value
    FROM procedures AS p
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(p.rowcount_metadata AS sql_variant)),
            ('procedures_last_90d', CAST(COALESCE(p.procedures_last_90d, 0) AS sql_variant)),
            ('null_cpt_last_90d', CAST(COALESCE(p.null_cpt_last_90d, 0) AS sql_variant))
    ) AS m(metric, value);

    WITH lab_results AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            lab_results_last_30d = SUM(CASE WHEN TRY_CONVERT(date, test_date) >= @start_30 THEN 1 ELSE 0 END),
            result_sum_last_30d = SUM(
                                           CASE
                                               WHEN TRY_CONVERT(date, test_date) >= @start_30
                                               THEN COALESCE(TRY_CONVERT(decimal(18, 6), result_value), 0)
                                               ELSE 0
                                           END
                                       ),
            result_count_last_30d = SUM(
                                             CASE
                                                 WHEN TRY_CONVERT(date, test_date) >= @start_30
                                                      AND TRY_CONVERT(decimal(18, 6), result_value) IS NOT NULL
                                                 THEN 1
                                                 ELSE 0
                                             END
                                         )
        FROM stg_optimized.lab_results_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.lab_results_std',
        m.metric,
        m.value
    FROM lab_results AS l
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(l.rowcount_metadata AS sql_variant)),
            ('lab_results_last_30d', CAST(COALESCE(l.lab_results_last_30d, 0) AS sql_variant)),
            (
                'avg_result_value_last_30d',
                CAST(
                    CASE
                        WHEN COALESCE(l.result_count_last_30d, 0) > 0
                        THEN COALESCE(l.result_sum_last_30d, 0)
                             / CONVERT(decimal(18, 6), l.result_count_last_30d)
                        ELSE CONVERT(decimal(18, 6), 0)
                    END AS sql_variant
                )
            )
    ) AS m(metric, value);

    WITH providers AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            active_providers = COUNT_BIG(*),
            experience_sum_ge5 = SUM(
                                          CASE
                                              WHEN TRY_CONVERT(decimal(18, 2), years_experience) >= 5
                                              THEN COALESCE(TRY_CONVERT(decimal(18, 2), years_experience), 0)
                                              ELSE 0
                                          END
                                      ),
            experience_count_ge5 = SUM(
                                           CASE
                                               WHEN TRY_CONVERT(decimal(18, 2), years_experience) >= 5
                                                    AND TRY_CONVERT(decimal(18, 2), years_experience) IS NOT NULL
                                               THEN 1
                                               ELSE 0
                                           END
                                       )
        FROM stg_optimized.providers_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.providers_std',
        m.metric,
        m.value
    FROM providers AS pr
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(pr.rowcount_metadata AS sql_variant)),
            ('active_providers', CAST(pr.active_providers AS sql_variant)),
            (
                'avg_experience_ge5',
                CAST(
                    CASE
                        WHEN COALESCE(pr.experience_count_ge5, 0) > 0
                        THEN COALESCE(pr.experience_sum_ge5, 0)
                             / CONVERT(decimal(18, 2), pr.experience_count_ge5)
                        ELSE CONVERT(decimal(18, 2), 0)
                    END AS sql_variant
                )
            )
    ) AS m(metric, value);

    WITH departments AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            high_occupancy_units = SUM(
                                          CASE
                                              WHEN TRY_CONVERT(decimal(9, 4), avg_occupancy) >= 0.85 THEN 1
                                              ELSE 0
                                          END
                                      ),
            beds_high_occupancy = SUM(
                                          CASE
                                              WHEN TRY_CONVERT(decimal(9, 4), avg_occupancy) >= 0.85
                                              THEN COALESCE(TRY_CONVERT(bigint, bed_count), 0)
                                              ELSE 0
                                          END
                                      )
        FROM stg_optimized.departments_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.departments_std',
        m.metric,
        m.value
    FROM departments AS dpt
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(dpt.rowcount_metadata AS sql_variant)),
            ('high_occupancy_units', CAST(COALESCE(dpt.high_occupancy_units, 0) AS sql_variant)),
            ('beds_high_occupancy', CAST(COALESCE(dpt.beds_high_occupancy, 0) AS sql_variant))
    ) AS m(metric, value);

    WITH hospitals AS
    (
        SELECT
            rowcount_metadata = COUNT_BIG(*),
            beds_total = SUM(COALESCE(TRY_CONVERT(bigint, total_beds), 0)),
            icu_pct_sum = SUM(COALESCE(TRY_CONVERT(decimal(5, 2), icu_percentage), 0)),
            icu_pct_count = SUM(
                                    CASE
                                        WHEN TRY_CONVERT(decimal(5, 2), icu_percentage) IS NOT NULL
                                        THEN 1
                                        ELSE 0
                                    END
                                )
        FROM stg_optimized.hospitals_std
    )
    INSERT INTO @results (object_name, metric, value)
    SELECT
        'stg_optimized.hospitals_std',
        m.metric,
        m.value
    FROM hospitals AS h
    CROSS APPLY
    (
        VALUES
            ('rowcount_metadata', CAST(h.rowcount_metadata AS sql_variant)),
            ('beds_total', CAST(COALESCE(h.beds_total, 0) AS sql_variant)),
            (
                'avg_icu_pct_recent',
                CAST(
                    CASE
                        WHEN COALESCE(h.icu_pct_count, 0) > 0
                        THEN COALESCE(h.icu_pct_sum, 0)
                             / CONVERT(decimal(5, 2), h.icu_pct_count)
                        ELSE CONVERT(decimal(5, 2), 0)
                    END AS sql_variant
                )
            )
    ) AS m(metric, value);

    DECLARE @insert_utc datetime2(7) = SYSUTCDATETIME();

    INSERT INTO ops.benchmarks (object_name, metric, value, run_utc, plan_id)
    SELECT object_name, metric, value, @insert_utc, @plan_id
    FROM @results;

    SET @rows_inserted = @@ROWCOUNT;

    COMMIT;

    PRINT 'staging_healthcheck inserted ' + CONVERT(varchar(12), @rows_inserted)
          + ' rows for plan_id ' + CONVERT(nvarchar(36), @plan_id);

    SELECT
        plan_id,
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
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK;

    IF @reset_plan_after_run = 1
    BEGIN
        EXEC sys.sp_set_session_context @key = N'ops_plan_id', @value = NULL, @read_only = 0;
        EXEC sys.sp_set_session_context @key = N'ops_plan_origin', @value = NULL, @read_only = 0;
    END;
    THROW;
END CATCH;
