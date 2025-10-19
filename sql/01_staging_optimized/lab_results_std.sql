/*
    Source table: dbo.lab_results
    Transformed columns: lab_result_id, admission_id, test_date, test_name,
        result_value, unit, reference_range, status, lab_technician
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED'
    Date styles used: 23, 111, 105, 103, 110, 101
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.lab_results_std
AS
    WITH
        source
        AS
        (
            SELECT
                lab_result_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.lab_result_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                admission_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.admission_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                test_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.test_date), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                test_name_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.test_name), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                result_value_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.result_value), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                unit_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.unit), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                reference_range_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.reference_range), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                status_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.status), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                lab_technician_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), lr.lab_technician), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.lab_results AS lr
        ),
        tokens
        AS
        (
            SELECT
                lab_result_id_token = CASE
            WHEN lab_result_id_text IS NULL OR lab_result_id_text = ''
                    OR UPPER(lab_result_id_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE REPLACE(lab_result_id_text, ',', '')
        END,
                admission_id_token = CASE
            WHEN admission_id_text IS NULL OR admission_id_text = ''
                    OR UPPER(admission_id_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE REPLACE(admission_id_text, ',', '')
        END,
                test_date_token = CASE
            WHEN test_date_text IS NULL OR test_date_text = ''
                    OR UPPER(test_date_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE test_date_text
        END,
                test_name_token = CASE
            WHEN test_name_text IS NULL OR test_name_text = ''
                    OR UPPER(test_name_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE test_name_text
        END,
                result_value_token = CASE
            WHEN result_value_text IS NULL OR result_value_text = ''
                    OR UPPER(result_value_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE REPLACE(REPLACE(result_value_text, ',', ''), ' ', '')
        END,
                unit_token = CASE
            WHEN unit_text IS NULL OR unit_text = ''
                    OR UPPER(unit_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE unit_text
        END,
                reference_range_token = CASE
            WHEN reference_range_text IS NULL OR reference_range_text = ''
                    OR UPPER(reference_range_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE reference_range_text
        END,
                status_token = CASE
            WHEN status_text IS NULL OR status_text = ''
                    OR UPPER(status_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE status_text
        END,
                lab_technician_token = CASE
            WHEN lab_technician_text IS NULL OR lab_technician_text = ''
                    OR UPPER(lab_technician_text) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
            THEN NULL
            ELSE lab_technician_text
        END
            FROM source
        ),
        typed
        AS
        (
            SELECT
                lab_result_id = TRY_CONVERT(int, lab_result_id_token),
                admission_id = TRY_CONVERT(int, admission_id_token),
                test_date = COALESCE(
            TRY_CONVERT(date, test_date_token, 23),
            TRY_CONVERT(date, test_date_token, 111),
            TRY_CONVERT(date, test_date_token, 105),
            TRY_CONVERT(date, test_date_token, 103),
            TRY_CONVERT(date, test_date_token, 110),
            TRY_CONVERT(date, test_date_token, 101)
        ),
                test_name = CASE
            WHEN test_name_token IS NULL THEN NULL
            ELSE CAST(REPLACE(REPLACE(REPLACE(test_name_token, '  ', ' '), '  ', ' '), '  ', ' ') AS nvarchar(120))
        END,
                result_value = TRY_CONVERT(decimal(18, 4), result_value_token),
                unit = CASE WHEN unit_token IS NULL THEN NULL ELSE CAST(unit_token AS nvarchar(20)) END,
                reference_range = CASE
            WHEN reference_range_token IS NULL THEN NULL
            ELSE CAST(
                REPLACE(
                    REPLACE(
                        REPLACE(REPLACE(LTRIM(RTRIM(reference_range_token)), ' - ', '-'), '- ', '-'),
                        ' -', '-'),
                    '  ', ' '
                )
            AS nvarchar(60))
        END,
                status = CASE
            WHEN status_token IS NULL THEN NULL
            ELSE CASE
                WHEN LEN(status_token) = 1 THEN UPPER(status_token)
                ELSE CONCAT(UPPER(LEFT(status_token, 1)), LOWER(SUBSTRING(status_token, 2, LEN(status_token))))
            END
        END,
                lab_technician = CASE
            WHEN lab_technician_token IS NULL THEN NULL
            ELSE CAST(CONCAT(UPPER(LEFT(lab_technician_token, 1)), LOWER(SUBSTRING(lab_technician_token, 2, LEN(lab_technician_token)))) AS nvarchar(80))
        END
            FROM tokens
        )
    SELECT
        lab_result_id,
        admission_id,
        test_date,
        test_name,
        result_value,
        unit,
        reference_range,
        status,
        lab_technician
    FROM typed
    WHERE lab_result_id IS NOT NULL
        AND admission_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no FORMAT, no CROSS APPLY.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.lab_results_std ORDER BY test_date DESC;
