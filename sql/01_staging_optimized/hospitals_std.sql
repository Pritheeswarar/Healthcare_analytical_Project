/*
    Source table: dbo.hospitals
    Transformed columns: hospital_id, hospital_name, branch_location, total_beds,
        ICU_bed_breakdown, General_bed_breakdown, Private_bed_breakdown,
        Semi_Private_bed_breakdown, icu_percentage, established_year, accreditation
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'TBD', '-'
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.hospitals_std
AS
    WITH
        source
        AS
        (
            SELECT
                hospital_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.hospital_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                hospital_name_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.hospital_name), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                branch_location_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.branch_location), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                total_beds_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.total_beds), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                icu_bed_breakdown_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.ICU_bed_breakdown), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                general_bed_breakdown_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.General_bed_breakdown), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                private_bed_breakdown_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.Private_bed_breakdown), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                semi_private_bed_breakdown_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.Semi_Private_bed_breakdown), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                icu_percentage_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.icu_percentage), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                established_year_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.established_year), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                accreditation_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), h.accreditation), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.hospitals AS h
        ),
        tokens
        AS
        (
            SELECT
                hospital_id_token = CASE
            WHEN hospital_id_text IS NULL OR hospital_id_text = ''
                    OR UPPER(hospital_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(hospital_id_text, ',', '')
        END,
                hospital_name_token = CASE
            WHEN hospital_name_text IS NULL OR hospital_name_text = ''
                    OR UPPER(hospital_name_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE hospital_name_text
        END,
                branch_location_token = CASE
            WHEN branch_location_text IS NULL OR branch_location_text = ''
                    OR UPPER(branch_location_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE branch_location_text
        END,
                total_beds_token = CASE
            WHEN total_beds_text IS NULL OR total_beds_text = ''
                    OR UPPER(total_beds_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(total_beds_text, ',', '')
        END,
                icu_bed_breakdown_token = CASE
            WHEN icu_bed_breakdown_text IS NULL OR icu_bed_breakdown_text = ''
                    OR UPPER(icu_bed_breakdown_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(icu_bed_breakdown_text, ',', '')
        END,
                general_bed_breakdown_token = CASE
            WHEN general_bed_breakdown_text IS NULL OR general_bed_breakdown_text = ''
                    OR UPPER(general_bed_breakdown_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(general_bed_breakdown_text, ',', '')
        END,
                private_bed_breakdown_token = CASE
            WHEN private_bed_breakdown_text IS NULL OR private_bed_breakdown_text = ''
                    OR UPPER(private_bed_breakdown_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(private_bed_breakdown_text, ',', '')
        END,
                semi_private_bed_breakdown_token = CASE
            WHEN semi_private_bed_breakdown_text IS NULL OR semi_private_bed_breakdown_text = ''
                    OR UPPER(semi_private_bed_breakdown_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(semi_private_bed_breakdown_text, ',', '')
        END,
                icu_percentage_token = CASE
            WHEN icu_percentage_text IS NULL OR icu_percentage_text = ''
                    OR UPPER(icu_percentage_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(icu_percentage_text, ',', '')
        END,
                established_year_token = CASE
            WHEN established_year_text IS NULL OR established_year_text = ''
                    OR UPPER(established_year_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(established_year_text, ',', '')
        END,
                accreditation_token = CASE
            WHEN accreditation_text IS NULL OR accreditation_text = ''
                    OR UPPER(accreditation_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE accreditation_text
        END
            FROM source
        ),
        typed
        AS
        (
            SELECT
                hospital_id = TRY_CONVERT(tinyint, hospital_id_token),
                hospital_name = CASE
            WHEN hospital_name_token IS NULL THEN NULL
            ELSE CAST(CONCAT(UPPER(LEFT(hospital_name_token, 1)), LOWER(SUBSTRING(hospital_name_token, 2, LEN(hospital_name_token)))) AS nvarchar(120))
        END,
                branch_location = CASE
            WHEN branch_location_token IS NULL THEN NULL
            ELSE CAST(CONCAT(UPPER(LEFT(branch_location_token, 1)), LOWER(SUBSTRING(branch_location_token, 2, LEN(branch_location_token)))) AS nvarchar(80))
        END,
                total_beds = TRY_CONVERT(smallint, total_beds_token),
                ICU_bed_breakdown = TRY_CONVERT(tinyint, icu_bed_breakdown_token),
                General_bed_breakdown = TRY_CONVERT(tinyint, general_bed_breakdown_token),
                Private_bed_breakdown = TRY_CONVERT(tinyint, private_bed_breakdown_token),
                Semi_Private_bed_breakdown = TRY_CONVERT(tinyint, semi_private_bed_breakdown_token),
                icu_percentage = TRY_CONVERT(decimal(5, 2), icu_percentage_token),
                established_year = TRY_CONVERT(smallint, established_year_token),
                accreditation = CASE
            WHEN accreditation_token IS NULL THEN NULL
            ELSE CASE UPPER(accreditation_token)
                WHEN 'JCI' THEN N'JCI'
                WHEN 'NABH' THEN N'NABH'
                WHEN 'NONE' THEN N'None'
                ELSE CAST(accreditation_token AS nvarchar(20))
            END
        END
            FROM tokens
        )
    SELECT
        hospital_id,
        hospital_name,
        branch_location,
        total_beds,
        ICU_bed_breakdown,
        General_bed_breakdown,
        Private_bed_breakdown,
        Semi_Private_bed_breakdown,
        icu_percentage,
        established_year,
        accreditation
    FROM typed
    WHERE hospital_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no FORMAT, no CROSS APPLY.
-- Confirm: identifier columns remain numeric.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.hospitals_std ORDER BY hospital_id;
