USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.hospitals_std
AS
    /*
    Staging view over dbo.hospitals applying datatype standardization and light text cleanup.
    Placeholders collapse to NULL; TRY_CONVERT is used for resilient typing.
*/
    SELECT
        typed.hospital_id,
        typed.hospital_name,
        typed.branch_location,
        typed.total_beds,
        typed.ICU_bed_breakdown,
        typed.General_bed_breakdown,
        typed.Private_bed_breakdown,
        typed.Semi_Private_bed_breakdown,
        typed.icu_percentage,
        typed.established_year,
        typed.accreditation
    FROM dbo.hospitals AS h
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.hospital_id))), '') AS hospital_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.hospital_name))), '') AS hospital_name_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.branch_location))), '') AS branch_location_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.total_beds))), '') AS total_beds_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.ICU_bed_breakdown))), '') AS ICU_bed_breakdown_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.General_bed_breakdown))), '') AS General_bed_breakdown_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.Private_bed_breakdown))), '') AS Private_bed_breakdown_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.Semi_Private_bed_breakdown))), '') AS Semi_Private_bed_breakdown_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.icu_percentage))), '') AS icu_percentage_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.established_year))), '') AS established_year_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), h.accreditation))), '') AS accreditation_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE WHEN trimmed.hospital_id_raw IS NULL OR UPPER(trimmed.hospital_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.hospital_id_raw, ',', '') END AS hospital_id_token,
            CASE WHEN trimmed.hospital_name_raw IS NULL OR UPPER(trimmed.hospital_name_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.hospital_name_raw END AS hospital_name_token,
            CASE WHEN trimmed.branch_location_raw IS NULL OR UPPER(trimmed.branch_location_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.branch_location_raw END AS branch_location_token,
            CASE WHEN trimmed.total_beds_raw IS NULL OR UPPER(trimmed.total_beds_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.total_beds_raw, ',', '') END AS total_beds_token,
            CASE WHEN trimmed.ICU_bed_breakdown_raw IS NULL OR UPPER(trimmed.ICU_bed_breakdown_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.ICU_bed_breakdown_raw, ',', '') END AS ICU_bed_breakdown_token,
            CASE WHEN trimmed.General_bed_breakdown_raw IS NULL OR UPPER(trimmed.General_bed_breakdown_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.General_bed_breakdown_raw, ',', '') END AS General_bed_breakdown_token,
            CASE WHEN trimmed.Private_bed_breakdown_raw IS NULL OR UPPER(trimmed.Private_bed_breakdown_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.Private_bed_breakdown_raw, ',', '') END AS Private_bed_breakdown_token,
            CASE WHEN trimmed.Semi_Private_bed_breakdown_raw IS NULL OR UPPER(trimmed.Semi_Private_bed_breakdown_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.Semi_Private_bed_breakdown_raw, ',', '') END AS Semi_Private_bed_breakdown_token,
            CASE WHEN trimmed.icu_percentage_raw IS NULL OR UPPER(trimmed.icu_percentage_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.icu_percentage_raw, ',', '') END AS icu_percentage_token,
            CASE WHEN trimmed.established_year_raw IS NULL OR UPPER(trimmed.established_year_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.established_year_raw, ',', '') END AS established_year_token,
            CASE WHEN trimmed.accreditation_raw IS NULL OR UPPER(trimmed.accreditation_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.accreditation_raw END AS accreditation_token
) AS tokens
CROSS APPLY (
    SELECT
            TRY_CONVERT(tinyint, tokens.hospital_id_token) AS hospital_id,
            CASE
            WHEN tokens.hospital_name_token IS NULL THEN NULL
            ELSE CAST(
                CONCAT(
                    UPPER(LEFT(tokens.hospital_name_token, 1)),
                    LOWER(SUBSTRING(tokens.hospital_name_token, 2, LEN(tokens.hospital_name_token)))
                )
            AS nvarchar(120))
        END AS hospital_name,
            CASE
            WHEN tokens.branch_location_token IS NULL THEN NULL
            ELSE CAST(
                CONCAT(
                    UPPER(LEFT(tokens.branch_location_token, 1)),
                    LOWER(SUBSTRING(tokens.branch_location_token, 2, LEN(tokens.branch_location_token)))
                )
            AS nvarchar(80))
        END AS branch_location,
            TRY_CONVERT(smallint, tokens.total_beds_token) AS total_beds,
            TRY_CONVERT(tinyint, tokens.ICU_bed_breakdown_token) AS ICU_bed_breakdown,
            TRY_CONVERT(tinyint, tokens.General_bed_breakdown_token) AS General_bed_breakdown,
            TRY_CONVERT(tinyint, tokens.Private_bed_breakdown_token) AS Private_bed_breakdown,
            TRY_CONVERT(tinyint, tokens.Semi_Private_bed_breakdown_token) AS Semi_Private_bed_breakdown,
            TRY_CONVERT(decimal(5,2), tokens.icu_percentage_token) AS icu_percentage,
            TRY_CONVERT(smallint, tokens.established_year_token) AS established_year,
            CASE
            WHEN tokens.accreditation_token IS NULL THEN NULL
            ELSE CASE UPPER(tokens.accreditation_token)
                    WHEN 'JCI' THEN N'JCI'
                    WHEN 'NABH' THEN N'NABH'
                    WHEN 'NONE' THEN N'None'
                    ELSE CAST(tokens.accreditation_token AS nvarchar(20))
                 END
        END AS accreditation
) AS typed
    WHERE typed.hospital_id IS NOT NULL;
GO
