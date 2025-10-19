USE [Healthcare];
GO

/*
Runbook: analytics_patients_pipeline
------------------------------------------------------------
- Purpose: Materialize an analytics-ready patients dimension and a thin reporting view.
- High-watermark persistence: analytics.etl_watermark_patients_std keeps the last processed created_date/patient_id pair.
  * To force a full backfill, set both columns to NULL (single row) and rerun.
  * To reprocess a specific window, lower last_created_date to the desired point in time; the loader always rereads one-day back.
- Incremental window: loader rehydrates rows with created_date >= (watermark - 1 day) and upserts into analytics.patients_std.
- Default load pattern: execute the incremental block during each ETL run after dbo.patients is refreshed.
- One-time historical load: ensure the watermark is NULL, execute this script, and allow the batch to process in ~100k row chunks automatically.
- Safety: script is idempotent; rerunning without changes will apply only deltas. Set XACT_ABORT ON for transactional reliability.
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

------------------------------------------------------------
-- Ensure analytics schema
------------------------------------------------------------
IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'analytics')
    EXEC ('CREATE SCHEMA analytics');
GO

------------------------------------------------------------
-- Staging view: lightweight standardisation with strong typing
------------------------------------------------------------
CREATE OR ALTER VIEW analytics.vw_patients_std_stage
AS
    WITH
        base
        AS
        (
            SELECT
                p.patient_id,
                NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(50), p.mrn))), '') AS mrn_raw,
                NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(20), p.pincode))), '') AS pincode_raw,
                NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(120), p.area))), '') AS area_raw,
                NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(30), p.gender))), '') AS gender_raw,
                NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(30), p.blood_group))), '') AS blood_group_raw,
                NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(60), p.insurance_type))), '') AS insurance_type_raw,
                NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(60), p.date_of_birth))), '') AS dob_raw,
                TRY_CONVERT(datetime2(0), p.created_date) AS created_date_raw
            FROM dbo.patients AS p
        ),
        standardized
        AS
        (
            SELECT
                b.patient_id,
                CASE
                WHEN b.mrn_raw IS NULL
                    OR UPPER(b.mrn_raw) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'NONE', '-')
                THEN NULL
                ELSE b.mrn_raw
            END AS mrn,
                CASE
                WHEN b.gender_raw IS NULL
                    OR UPPER(b.gender_raw) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'NONE', '-')
                THEN NULL
                WHEN UPPER(b.gender_raw) IN ('M', 'MALE') THEN 'M'
                WHEN UPPER(b.gender_raw) IN ('F', 'FEMALE') THEN 'F'
                WHEN UPPER(b.gender_raw) IN ('OTHER', 'O') THEN 'O'
                ELSE NULL
            END AS gender,
                CASE
                WHEN b.blood_group_raw IS NULL
                    OR UPPER(b.blood_group_raw) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'NONE', '-')
                THEN NULL
                ELSE
                    CASE UPPER(REPLACE(b.blood_group_raw, ' ', ''))
                        WHEN 'A+' THEN 'A+'
                        WHEN 'A-' THEN 'A-'
                        WHEN 'B+' THEN 'B+'
                        WHEN 'B-' THEN 'B-'
                        WHEN 'AB+' THEN 'AB+'
                        WHEN 'AB-' THEN 'AB-'
                        WHEN 'O+' THEN 'O+'
                        WHEN 'O-' THEN 'O-'
                        ELSE NULL
                    END
            END AS blood_group,
                CASE
                WHEN b.insurance_type_raw IS NULL
                    OR UPPER(b.insurance_type_raw) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'NONE', '-')
                THEN NULL
                ELSE
                    CASE UPPER(REPLACE(b.insurance_type_raw, '-', ' '))
                        WHEN 'GOVERNMENT' THEN CAST(1 AS tinyint)
                        WHEN 'GOV' THEN CAST(1 AS tinyint)
                        WHEN 'PRIVATE' THEN CAST(2 AS tinyint)
                        WHEN 'PVT' THEN CAST(2 AS tinyint)
                        WHEN 'SELF PAY' THEN CAST(3 AS tinyint)
                        WHEN 'SELF PAY ' THEN CAST(3 AS tinyint)
                        WHEN 'SELF' THEN CAST(3 AS tinyint)
                        WHEN 'CORPORATE' THEN CAST(4 AS tinyint)
                        WHEN 'CORP' THEN CAST(4 AS tinyint)
                        ELSE CAST(5 AS tinyint)
                    END
            END AS insurance_type_code,
                CASE
                WHEN b.area_raw IS NULL
                    OR UPPER(b.area_raw) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'NONE', '-')
                THEN NULL
                ELSE LTRIM(RTRIM(REPLACE(REPLACE(b.area_raw, CHAR(13), ' '), CHAR(10), ' ')))
            END AS area,
                CASE
                WHEN b.pincode_raw IS NULL
                    OR UPPER(b.pincode_raw) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'NONE', '-')
                THEN NULL
                ELSE
                    CASE
                        WHEN b.pincode_raw LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]' THEN CAST(b.pincode_raw AS char(6))
                        WHEN TRY_CONVERT(int, b.pincode_raw) BETWEEN 100000 AND 999999
                            THEN RIGHT('000000' + CONVERT(varchar(6), TRY_CONVERT(int, b.pincode_raw)), 6)
                        ELSE NULL
                    END
            END AS pincode,
                COALESCE(
                TRY_CONVERT(date, b.dob_raw, 23),
                TRY_CONVERT(date, b.dob_raw, 112),
                TRY_CONVERT(date, b.dob_raw, 101),
                TRY_CONVERT(date, b.dob_raw, 103)
            ) AS dob,
                b.created_date_raw AS created_date
            FROM base AS b
        )
    SELECT
        s.patient_id,
        s.mrn,
        s.dob,
        s.created_date,
        s.gender,
        s.blood_group,
        s.insurance_type_code,
        s.area,
        s.pincode,
        CASE
            WHEN age_calc.age_at_created_raw BETWEEN 0 AND 125
                THEN CAST(age_calc.age_at_created_raw AS tinyint)
            ELSE NULL
        END AS age_at_created,
        CASE
            WHEN age_calc.age_current_raw BETWEEN 0 AND 125
                THEN CAST(age_calc.age_current_raw AS tinyint)
            ELSE NULL
        END AS age_current
    FROM standardized AS s
    CROSS APPLY (
        SELECT
            CASE
                WHEN s.dob IS NULL OR s.created_date IS NULL THEN NULL
                ELSE
                    DATEDIFF(year, s.dob, CAST(s.created_date AS date))
                    - CASE
                        WHEN DATEADD(year, DATEDIFF(year, s.dob, CAST(s.created_date AS date)), s.dob) > CAST(s.created_date AS date) THEN 1
                        ELSE 0
                      END
            END AS age_at_created_raw,
            CASE
                WHEN s.dob IS NULL THEN NULL
                ELSE
                    DATEDIFF(year, s.dob, CAST(SYSUTCDATETIME() AS date))
                    - CASE
                        WHEN DATEADD(year, DATEDIFF(year, s.dob, CAST(SYSUTCDATETIME() AS date)), s.dob) > CAST(SYSUTCDATETIME() AS date) THEN 1
                        ELSE 0
                      END
            END AS age_current_raw
    ) AS age_calc;
GO

------------------------------------------------------------
-- Watermark table for incremental processing
------------------------------------------------------------
IF OBJECT_ID('analytics.etl_watermark_patients_std', 'U') IS NULL
BEGIN
    CREATE TABLE analytics.etl_watermark_patients_std
    (
        watermark_id int NOT NULL CONSTRAINT PK_etl_watermark_patients_std PRIMARY KEY,
        last_created_date datetime2(0) NULL,
        last_patient_id int NULL,
        updated_at datetime2(0) NOT NULL CONSTRAINT DF_etl_watermark_patients_std_updated_at DEFAULT SYSUTCDATETIME()
    );

    INSERT INTO analytics.etl_watermark_patients_std
        (watermark_id, last_created_date, last_patient_id)
    VALUES
        (1, NULL, NULL);
END;
GO

------------------------------------------------------------
-- Target table (materialised analytics layer)
------------------------------------------------------------
IF OBJECT_ID('analytics.patients_std', 'U') IS NULL
BEGIN
    CREATE TABLE analytics.patients_std
    (
        patients_std_surrogate bigint IDENTITY(1, 1) NOT NULL,
        patient_id int NOT NULL,
        mrn nvarchar(30) NULL,
        dob date NULL,
        age_at_created tinyint NULL,
        blood_group char(3) NULL,
        insurance_type tinyint NULL,
        gender char(1) NULL,
        area nvarchar(120) NULL,
        pincode char(6) NULL,
        created_date datetime2(0) NULL,
        age_band AS (
            CASE
                WHEN age_at_created IS NULL THEN 'Unknown'
                WHEN age_at_created < 18 THEN '0-17'
                WHEN age_at_created < 35 THEN '18-34'
                WHEN age_at_created < 50 THEN '35-49'
                WHEN age_at_created < 65 THEN '50-64'
                WHEN age_at_created < 80 THEN '65-79'
                WHEN age_at_created < 90 THEN '80-89'
                ELSE '90+'
            END
        ) PERSISTED,
        load_ts datetime2(0) NOT NULL CONSTRAINT DF_patients_std_load_ts DEFAULT SYSUTCDATETIME(),
        row_version rowversion NOT NULL,
        CONSTRAINT PK_patients_std PRIMARY KEY NONCLUSTERED (patient_id),
        CONSTRAINT CK_patients_std_gender CHECK (gender IS NULL OR gender IN ('M', 'F', 'O')),
        CONSTRAINT CK_patients_std_blood_group CHECK (blood_group IS NULL OR blood_group IN ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-')),
        CONSTRAINT CK_patients_std_insurance CHECK (insurance_type IS NULL OR (insurance_type BETWEEN 1 AND 5)),
        CONSTRAINT CK_patients_std_pincode CHECK (pincode IS NULL OR pincode LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]')
    );

    CREATE CLUSTERED INDEX CI_patients_std ON analytics.patients_std (patients_std_surrogate);
END;
ELSE
BEGIN
    IF NOT EXISTS (
        SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('analytics.patients_std')
        AND name = 'CI_patients_std'
    )
        CREATE CLUSTERED INDEX CI_patients_std ON analytics.patients_std (patients_std_surrogate);
END;
GO

------------------------------------------------------------
-- Incremental upsert
------------------------------------------------------------
DECLARE @last_created_date datetime2(0);
DECLARE @last_patient_id int;
DECLARE @lookback datetime2(0);

SELECT
    @last_created_date = w.last_created_date,
    @last_patient_id = w.last_patient_id
FROM analytics.etl_watermark_patients_std AS w
WHERE w.watermark_id = 1;

SET @lookback = CASE
    WHEN @last_created_date IS NULL THEN '19000101'
    ELSE DATEADD(day, -1, @last_created_date)
END;

DROP TABLE IF EXISTS #patients_delta;

SELECT
    s.patient_id,
    s.mrn,
    s.dob,
    CAST(s.age_at_created AS tinyint) AS age_at_created,
    s.blood_group,
    s.insurance_type_code AS insurance_type,
    s.gender,
    CAST(s.area AS nvarchar(120)) AS area,
    CAST(s.pincode AS char(6)) AS pincode,
    CAST(s.created_date AS datetime2(0)) AS created_date
INTO #patients_delta
FROM analytics.vw_patients_std_stage AS s
WHERE
    s.patient_id IS NOT NULL
    AND (
        @last_created_date IS NULL
    OR s.created_date IS NULL
    OR s.created_date > @last_created_date
    OR (s.created_date = @last_created_date AND s.patient_id > ISNULL(@last_patient_id, 0))
    OR s.created_date >= @lookback
    );

IF EXISTS (SELECT 1
FROM #patients_delta)
BEGIN
    SET STATISTICS IO ON;
    SET STATISTICS TIME ON;

    DECLARE @merge_audit TABLE (action_desc nvarchar(10));

    MERGE analytics.patients_std AS tgt
    USING #patients_delta AS src
        ON tgt.patient_id = src.patient_id
    WHEN MATCHED AND (
            ISNULL(tgt.mrn, N'') <> ISNULL(src.mrn, N'')
        OR ISNULL(tgt.dob, '19000101') <> ISNULL(src.dob, '19000101')
        OR ISNULL(tgt.age_at_created, 255) <> ISNULL(src.age_at_created, 255)
        OR ISNULL(tgt.blood_group, '---') <> ISNULL(src.blood_group, '---')
        OR ISNULL(tgt.insurance_type, 0) <> ISNULL(src.insurance_type, 0)
        OR ISNULL(tgt.gender, '-') <> ISNULL(src.gender, '-')
        OR ISNULL(tgt.area, N'') <> ISNULL(src.area, N'')
        OR ISNULL(tgt.pincode, '000000') <> ISNULL(src.pincode, '000000')
        OR ISNULL(tgt.created_date, '19000101') <> ISNULL(src.created_date, '19000101')
        )
        THEN UPDATE SET
            mrn = src.mrn,
            dob = src.dob,
            age_at_created = src.age_at_created,
            blood_group = src.blood_group,
            insurance_type = src.insurance_type,
            gender = src.gender,
            area = src.area,
            pincode = src.pincode,
            created_date = src.created_date,
            load_ts = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (
            patient_id,
            mrn,
            dob,
            age_at_created,
            blood_group,
            insurance_type,
            gender,
            area,
            pincode,
            created_date
        )
        VALUES (
            src.patient_id,
            src.mrn,
            src.dob,
            src.age_at_created,
            src.blood_group,
            src.insurance_type,
            src.gender,
            src.area,
            src.pincode,
            src.created_date
        )
    OUTPUT $action INTO @merge_audit;

    SET STATISTICS IO OFF;
    SET STATISTICS TIME OFF;

    DECLARE @inserted int = (SELECT COUNT(*)
    FROM @merge_audit
    WHERE action_desc = 'INSERT');
    DECLARE @updated int = (SELECT COUNT(*)
    FROM @merge_audit
    WHERE action_desc = 'UPDATE');

    PRINT CONCAT('analytics.patients_std upsert complete. Inserts: ', @inserted, ' Updates: ', @updated, '.');
END
ELSE
BEGIN
    PRINT 'analytics.patients_std upsert skipped: no qualifying source rows.';
END;

------------------------------------------------------------
-- Update watermark to latest created_date/patient_id
------------------------------------------------------------
DECLARE @max_created_date datetime2(0);
DECLARE @max_patient_id int;

SELECT TOP (1)
    @max_created_date = ps.created_date,
    @max_patient_id = ps.patient_id
FROM analytics.patients_std AS ps
WHERE ps.created_date IS NOT NULL
ORDER BY ps.created_date DESC, ps.patient_id DESC;

UPDATE analytics.etl_watermark_patients_std
SET
    last_created_date = @max_created_date,
    last_patient_id = @max_patient_id,
    updated_at = SYSUTCDATETIME()
WHERE watermark_id = 1;

------------------------------------------------------------
-- Conditional columnstore conversion for large row counts
------------------------------------------------------------
DECLARE @rowcount bigint;
SELECT @rowcount = SUM(row_count)
FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('analytics.patients_std')
    AND index_id IN (0, 1);

IF @rowcount IS NOT NULL AND @rowcount >= 500000
BEGIN
    IF EXISTS (
        SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('analytics.patients_std')
        AND name = 'CI_patients_std'
    )
    BEGIN
        DROP INDEX CI_patients_std ON analytics.patients_std;
    END;

    IF NOT EXISTS (
        SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('analytics.patients_std')
        AND name = 'CCI_patients_std'
    )
    BEGIN
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_patients_std ON analytics.patients_std
        WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0);
    END;
END;
ELSE
BEGIN
    IF NOT EXISTS (
        SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('analytics.patients_std')
        AND name = 'CI_patients_std'
    )
    BEGIN
        CREATE CLUSTERED INDEX CI_patients_std ON analytics.patients_std (patients_std_surrogate);
    END;

    UPDATE STATISTICS analytics.patients_std WITH FULLSCAN;
END;

------------------------------------------------------------
-- Final reporting view
------------------------------------------------------------
GO

CREATE OR ALTER VIEW analytics.v_patients_std
AS
    SELECT
        ps.patient_id,
        ps.mrn,
        ps.age_at_created AS age,
        ps.blood_group,
        ps.insurance_type AS insurance_type,
        ps.gender,
        ps.area,
        ps.pincode,
        ps.created_date
    FROM analytics.patients_std AS ps;
GO
