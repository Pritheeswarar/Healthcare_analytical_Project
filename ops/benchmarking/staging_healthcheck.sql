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
        plan_id uniqueidentifier NULL
    );
END;
GO

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
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @utc_now datetime2(7) = SYSUTCDATETIME();
DECLARE @start_30 date = DATEADD(DAY, -30, CAST(@utc_now AS date));
DECLARE @start_90 date = DATEADD(DAY, -90, CAST(@utc_now AS date));

BEGIN TRY
    BEGIN TRAN;

    DECLARE @results TABLE
    (
        object_name sysname NOT NULL,
        metric nvarchar(200) NOT NULL,
        value sql_variant NOT NULL
    );

    WITH admissions AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            admissions_last_30d = COALESCE(SUM(CASE WHEN TRY_CONVERT(date, admission_date) >= @start_30 THEN 1 ELSE 0 END), 0),
            avg_los_last_30d = COALESCE(
                                    AVG(
                                        CASE
                                            WHEN TRY_CONVERT(date, admission_date) >= @start_30
                                            THEN TRY_CONVERT(decimal(18, 4), length_of_stay)
                                        END
                                    ),
                                    CONVERT(decimal(18, 4), 0)
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
            ('rowcount_actual', CAST(a.rowcount_actual AS sql_variant)),
            ('admissions_last_30d', CAST(a.admissions_last_30d AS sql_variant)),
            ('avg_los_last_30d', CAST(a.avg_los_last_30d AS sql_variant))
    ) AS m(metric, value);

    WITH billing AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            total_amount_last_30d = COALESCE(
                                             SUM(
                                                 CASE
                                                     WHEN TRY_CONVERT(date, bill_date) >= @start_30
                                                     THEN COALESCE(TRY_CONVERT(decimal(19, 4), total_amount), 0)
                                                     ELSE 0
                                                 END
                                             ),
                                             CONVERT(decimal(19, 4), 0)
                                         ),
            patient_due_last_30d = COALESCE(
                                            SUM(
                                                CASE
                                                    WHEN TRY_CONVERT(date, bill_date) >= @start_30
                                                    THEN COALESCE(TRY_CONVERT(decimal(19, 4), patient_due), 0)
                                                    ELSE 0
                                                END
                                            ),
                                            CONVERT(decimal(19, 4), 0)
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
            ('rowcount_actual', CAST(b.rowcount_actual AS sql_variant)),
            ('total_amount_last_30d', CAST(b.total_amount_last_30d AS sql_variant)),
            ('patient_due_last_30d', CAST(b.patient_due_last_30d AS sql_variant))
    ) AS m(metric, value);

    WITH diagnoses AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            diagnoses_last_90d = COALESCE(SUM(CASE WHEN TRY_CONVERT(date, diagnosis_date) >= @start_90 THEN 1 ELSE 0 END), 0),
            null_icd_last_90d = COALESCE(SUM(
                                     CASE
                                         WHEN TRY_CONVERT(date, diagnosis_date) >= @start_90 AND icd_code IS NULL
                                         THEN 1
                                         ELSE 0
                                     END
                                 ), 0)
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
            ('rowcount_actual', CAST(d.rowcount_actual AS sql_variant)),
            ('diagnoses_last_90d', CAST(d.diagnoses_last_90d AS sql_variant)),
            ('null_icd_last_90d', CAST(d.null_icd_last_90d AS sql_variant))
    ) AS m(metric, value);

    WITH procedures AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            procedures_last_90d = COALESCE(SUM(CASE WHEN TRY_CONVERT(date, procedure_date) >= @start_90 THEN 1 ELSE 0 END), 0),
            null_cpt_last_90d = COALESCE(SUM(
                                     CASE
                                         WHEN TRY_CONVERT(date, procedure_date) >= @start_90 AND cpt_code IS NULL
                                         THEN 1
                                         ELSE 0
                                     END
                                 ), 0)
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
            ('rowcount_actual', CAST(p.rowcount_actual AS sql_variant)),
            ('procedures_last_90d', CAST(p.procedures_last_90d AS sql_variant)),
            ('null_cpt_last_90d', CAST(p.null_cpt_last_90d AS sql_variant))
    ) AS m(metric, value);

    WITH lab_results AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            lab_results_last_30d = COALESCE(SUM(CASE WHEN TRY_CONVERT(date, test_date) >= @start_30 THEN 1 ELSE 0 END), 0),
            avg_result_value_last_30d = COALESCE(
                                               AVG(
                                                   CASE
                                                       WHEN TRY_CONVERT(date, test_date) >= @start_30 AND result_value IS NOT NULL
                                                       THEN TRY_CONVERT(decimal(18, 6), result_value)
                                                   END
                                               ),
                                               CONVERT(decimal(18, 6), 0)
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
            ('rowcount_actual', CAST(l.rowcount_actual AS sql_variant)),
            ('lab_results_last_30d', CAST(l.lab_results_last_30d AS sql_variant)),
            ('avg_result_value_last_30d', CAST(l.avg_result_value_last_30d AS sql_variant))
    ) AS m(metric, value);

    WITH providers AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            active_providers = COUNT_BIG(1),
            avg_experience_ge5 = COALESCE(
                                          AVG(
                                              CASE
                                                  WHEN TRY_CONVERT(decimal(18, 2), years_experience) >= 5
                                                  THEN TRY_CONVERT(decimal(18, 2), years_experience)
                                              END
                                          ),
                                          CONVERT(decimal(18, 2), 0)
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
            ('rowcount_actual', CAST(pr.rowcount_actual AS sql_variant)),
            ('active_providers', CAST(pr.active_providers AS sql_variant)),
            ('avg_experience_ge5', CAST(pr.avg_experience_ge5 AS sql_variant))
    ) AS m(metric, value);

    WITH departments AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            high_occupancy_units = COALESCE(SUM(
                                          CASE
                                              WHEN TRY_CONVERT(decimal(9, 4), avg_occupancy) >= 0.85 THEN 1
                                              ELSE 0
                                          END
                                      ), 0),
            beds_high_occupancy = COALESCE(SUM(
                                          CASE
                                              WHEN TRY_CONVERT(decimal(9, 4), avg_occupancy) >= 0.85
                                              THEN COALESCE(TRY_CONVERT(bigint, bed_count), 0)
                                              ELSE 0
                                          END
                                      ), 0)
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
            ('rowcount_actual', CAST(dpt.rowcount_actual AS sql_variant)),
            ('high_occupancy_units', CAST(dpt.high_occupancy_units AS sql_variant)),
            ('beds_high_occupancy', CAST(dpt.beds_high_occupancy AS sql_variant))
    ) AS m(metric, value);

    WITH hospitals AS
    (
        SELECT
            rowcount_actual = COUNT_BIG(1),
            beds_total = COALESCE(SUM(COALESCE(TRY_CONVERT(bigint, total_beds), 0)), 0),
            avg_icu_pct_recent = COALESCE(
                                              AVG(TRY_CONVERT(decimal(5, 2), icu_percentage)),
                                              CONVERT(decimal(5, 2), 0)
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
            ('rowcount_actual', CAST(h.rowcount_actual AS sql_variant)),
            ('beds_total', CAST(h.beds_total AS sql_variant)),
            ('avg_icu_pct_recent', CAST(h.avg_icu_pct_recent AS sql_variant))
    ) AS m(metric, value);

    INSERT INTO ops.benchmarks (object_name, metric, value, plan_id)
    SELECT object_name, metric, value, NULL
    FROM @results;

    COMMIT;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK;
    THROW;
END CATCH;

SELECT TOP (1)
    plan_id = CONVERT(uniqueidentifier, NULL),
    note = 'staging_healthcheck complete',
    run_utc = SYSUTCDATETIME();
