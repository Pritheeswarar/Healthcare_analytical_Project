/*
    Purpose: Materialize xf.admissions to support ALOS and 30-day readmission analytics.
    Source:  stg_optimized.admissions_std
    Notes:   Deterministic, rerunnable script; keep logic lightweight for SQL Server Express.
*/
USE [Healthcare];
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'xf'
)
BEGIN
    EXEC('CREATE SCHEMA xf');
END;
GO

IF OBJECT_ID('xf.admissions', 'U') IS NOT NULL
BEGIN
    DROP TABLE xf.admissions;
END;
GO

CREATE TABLE xf.admissions
(
    admission_id int NOT NULL,
    patient_id int NOT NULL,
    hospital_id int NOT NULL,
    department_id int NULL,
    admission_date date NOT NULL,
    discharge_date date NULL,
    los_days int NULL,
    is_open_stay bit NOT NULL,
    is_transfer bit NOT NULL,
    index_discharge_date date NULL,
    load_ts datetime2(3) NOT NULL CONSTRAINT DF_xf_admissions_load_ts DEFAULT SYSDATETIME(),
    CONSTRAINT CK_xf_admissions_los_days_non_negative CHECK (los_days IS NULL OR los_days >= 0)
);
GO

SET NOCOUNT ON;

INSERT INTO xf.admissions
(
    admission_id,
    patient_id,
    hospital_id,
    department_id,
    admission_date,
    discharge_date,
    los_days,
    is_open_stay,
    is_transfer,
    index_discharge_date
)
SELECT
    src.admission_id,
    src.patient_id,
    src.hospital_id,
    src.department_id,
    src.admission_date_final AS admission_date,
    src.discharge_date_final AS discharge_date,
    CASE
        WHEN src.discharge_date_final IS NULL THEN NULL
        WHEN DATEDIFF(day, src.admission_date_final, src.discharge_date_final) < 0 THEN NULL
        ELSE DATEDIFF(day, src.admission_date_final, src.discharge_date_final)
    END AS los_days,
    CAST(IIF(src.discharge_date_final IS NULL, 1, 0) AS bit) AS is_open_stay,
    CAST(
        IIF(
            src.discharge_status IN (
                N'Transferred',
                N'Transfer',
                N'Transfer To Acute Care',
                N'Transfer to acute care',
                N'Referred',
                N'Skilled Nursing Facility',
                N'Rehabilitation'
            ),
            1,
            0
        ) AS bit
    ) AS is_transfer,
    src.discharge_date_final AS index_discharge_date
FROM stg_optimized.admissions_std AS src
WHERE src.admission_date_final IS NOT NULL;
GO

CREATE CLUSTERED INDEX CI_xf_admissions
ON xf.admissions (hospital_id, admission_date, admission_id);
GO

CREATE NONCLUSTERED INDEX NCI_xf_admissions_patient
ON xf.admissions (patient_id, admission_date)
INCLUDE (discharge_date, los_days, is_transfer);
GO

-- Safety Check 1: Expect zero rows (admission date later than discharge date).
SELECT
    admission_id,
    patient_id,
    admission_date,
    discharge_date
FROM xf.admissions
WHERE admission_date > discharge_date;
GO

-- Safety Check 2: Length-of-stay distribution (ignoring NULL).
SELECT TOP (1)
    MIN(los_days) OVER () AS los_min,
    CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY los_days) OVER () AS decimal(10, 2)) AS los_p50,
    CAST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY los_days) OVER () AS decimal(10, 2)) AS los_p95,
    MAX(los_days) OVER () AS los_max
FROM xf.admissions
WHERE los_days IS NOT NULL;
GO

-- Safety Check 3a: Open stay counts.
SELECT
    is_open_stay,
    COUNT(*) AS row_count
FROM xf.admissions
GROUP BY is_open_stay
ORDER BY is_open_stay DESC;
GO

-- Safety Check 3b: Transfer counts.
SELECT
    is_transfer,
    COUNT(*) AS row_count
FROM xf.admissions
GROUP BY is_transfer
ORDER BY is_transfer DESC;
GO
