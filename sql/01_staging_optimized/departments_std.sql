/*
    Source table: dbo.departments
    Transformed columns: department_id, hospital_id, dept_name, hod, bed_count, avg_occupancy
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'TBD', '-'
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.departments_std
AS
    WITH
        source
        AS
        (
            SELECT
                department_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.department_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                hospital_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.hospital_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                dept_name_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.dept_name), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                hod_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.hod), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                bed_count_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.bed_count), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                avg_occupancy_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.avg_occupancy), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.departments AS d
        ),
        tokens
        AS
        (
            SELECT
                department_id_token = CASE
            WHEN department_id_text IS NULL OR department_id_text = ''
                    OR UPPER(department_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(department_id_text, ',', '')
        END,
                hospital_id_token = CASE
            WHEN hospital_id_text IS NULL OR hospital_id_text = ''
                    OR UPPER(hospital_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(hospital_id_text, ',', '')
        END,
                dept_name_token = CASE
            WHEN dept_name_text IS NULL OR dept_name_text = ''
                    OR UPPER(dept_name_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE dept_name_text
        END,
                hod_token = CASE
            WHEN hod_text IS NULL OR hod_text = ''
                    OR UPPER(hod_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE hod_text
        END,
                bed_count_token = CASE
            WHEN bed_count_text IS NULL OR bed_count_text = ''
                    OR UPPER(bed_count_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(bed_count_text, ',', '')
        END,
                avg_occupancy_token = CASE
            WHEN avg_occupancy_text IS NULL OR avg_occupancy_text = ''
                    OR UPPER(avg_occupancy_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(avg_occupancy_text, ',', ''), ' ', '')
        END
            FROM source
        ),
        typed
        AS
        (
            SELECT
                department_id = TRY_CONVERT(tinyint, department_id_token),
                hospital_id = TRY_CONVERT(tinyint, hospital_id_token),
                dept_name = CASE
            WHEN dept_name_token IS NULL THEN NULL
            ELSE CAST(CONCAT(UPPER(LEFT(dept_name_token, 1)), LOWER(SUBSTRING(dept_name_token, 2, LEN(dept_name_token)))) AS nvarchar(100))
        END,
                hod = CASE
            WHEN hod_token IS NULL THEN NULL
            ELSE CAST(CONCAT(UPPER(LEFT(hod_token, 1)), LOWER(SUBSTRING(hod_token, 2, LEN(hod_token)))) AS nvarchar(80))
        END,
                bed_count = TRY_CONVERT(tinyint, bed_count_token),
                avg_occupancy = TRY_CONVERT(decimal(5, 2), avg_occupancy_token)
            FROM tokens
        )
    SELECT
        department_id,
        hospital_id,
        dept_name,
        hod,
        bed_count,
        avg_occupancy
    FROM typed
    WHERE department_id IS NOT NULL
        AND hospital_id IS NOT NULL
        AND bed_count IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no FORMAT, no CROSS APPLY.
-- Confirm: department and hospital identifiers remain required.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.departments_std ORDER BY department_id;
