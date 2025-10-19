USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.departments_std
AS
    /*
    Staging view over dbo.departments applying data typing and light text normalization only.
    Placeholder tokens collapse to NULL; TRY_CONVERT enforces safe typing for identifiers and metrics.
*/
    SELECT
        typed.department_id,
        typed.hospital_id,
        typed.dept_name,
        typed.hod,
        typed.bed_count,
        typed.avg_occupancy
    FROM dbo.departments AS d
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.department_id))), '') AS department_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.hospital_id))), '') AS hospital_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.dept_name))), '') AS dept_name_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.hod))), '') AS hod_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.bed_count))), '') AS bed_count_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.avg_occupancy))), '') AS avg_occupancy_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE WHEN trimmed.department_id_raw IS NULL OR UPPER(trimmed.department_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.department_id_raw, ',', '') END AS department_id_token,
            CASE WHEN trimmed.hospital_id_raw IS NULL OR UPPER(trimmed.hospital_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.hospital_id_raw, ',', '') END AS hospital_id_token,
            CASE WHEN trimmed.dept_name_raw IS NULL OR UPPER(trimmed.dept_name_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.dept_name_raw END AS dept_name_token,
            CASE WHEN trimmed.hod_raw IS NULL OR UPPER(trimmed.hod_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.hod_raw END AS hod_token,
            CASE WHEN trimmed.bed_count_raw IS NULL OR UPPER(trimmed.bed_count_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.bed_count_raw, ',', '') END AS bed_count_token,
            CASE WHEN trimmed.avg_occupancy_raw IS NULL OR UPPER(trimmed.avg_occupancy_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.avg_occupancy_raw, ',', ''), ' ', '') END AS avg_occupancy_token
) AS tokens
CROSS APPLY (
    SELECT
            TRY_CONVERT(tinyint, tokens.department_id_token) AS department_id,
            TRY_CONVERT(tinyint, tokens.hospital_id_token) AS hospital_id,
            CASE
            WHEN tokens.dept_name_token IS NULL THEN NULL
            ELSE CAST(
                CONCAT(
                    UPPER(LEFT(tokens.dept_name_token, 1)),
                    LOWER(SUBSTRING(tokens.dept_name_token, 2, LEN(tokens.dept_name_token)))
                )
            AS nvarchar(100))
        END AS dept_name,
            CASE
            WHEN tokens.hod_token IS NULL THEN NULL
            ELSE CAST(
                CONCAT(
                    UPPER(LEFT(tokens.hod_token, 1)),
                    LOWER(SUBSTRING(tokens.hod_token, 2, LEN(tokens.hod_token)))
                )
            AS nvarchar(80))
        END AS hod,
            TRY_CONVERT(tinyint, tokens.bed_count_token) AS bed_count,
            TRY_CONVERT(decimal(5,2), tokens.avg_occupancy_token) AS avg_occupancy
) AS typed
    WHERE typed.department_id IS NOT NULL
        AND typed.hospital_id IS NOT NULL
        AND typed.bed_count IS NOT NULL;
GO
