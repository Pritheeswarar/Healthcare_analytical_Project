USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.lab_results_std
AS
    /*
    Staging view over dbo.lab_results with standardization only.
    Placeholder tokens collapse to NULL; TRY_CONVERT enforces resilient typing.
*/
    SELECT
        typed.lab_result_id,
        typed.admission_id,
        typed.test_date,
        typed.test_name,
        typed.result_value,
        typed.unit,
        typed.reference_range,
        typed.status,
        typed.lab_technician
    FROM dbo.lab_results AS lr
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.lab_result_id))), '') AS lab_result_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.admission_id))), '') AS admission_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.test_date))), '') AS test_date_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.test_name))), '') AS test_name_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.result_value))), '') AS result_value_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.unit))), '') AS unit_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.reference_range))), '') AS reference_range_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.status))), '') AS status_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), lr.lab_technician))), '') AS lab_technician_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE WHEN trimmed.lab_result_id_raw IS NULL OR UPPER(trimmed.lab_result_id_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE REPLACE(trimmed.lab_result_id_raw, ',', '') END AS lab_result_id_token,
            CASE WHEN trimmed.admission_id_raw IS NULL OR UPPER(trimmed.admission_id_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE REPLACE(trimmed.admission_id_raw, ',', '') END AS admission_id_token,
            CASE WHEN trimmed.test_date_raw IS NULL OR UPPER(trimmed.test_date_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE trimmed.test_date_raw END AS test_date_token,
            CASE WHEN trimmed.test_name_raw IS NULL OR UPPER(trimmed.test_name_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE trimmed.test_name_raw END AS test_name_token,
            CASE WHEN trimmed.result_value_raw IS NULL OR UPPER(trimmed.result_value_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.result_value_raw, ',', ''), ' ', '') END AS result_value_token,
            CASE WHEN trimmed.unit_raw IS NULL OR UPPER(trimmed.unit_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE trimmed.unit_raw END AS unit_token,
            CASE WHEN trimmed.reference_range_raw IS NULL OR UPPER(trimmed.reference_range_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE trimmed.reference_range_raw END AS reference_range_token,
            CASE WHEN trimmed.status_raw IS NULL OR UPPER(trimmed.status_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE trimmed.status_raw END AS status_token,
            CASE WHEN trimmed.lab_technician_raw IS NULL OR UPPER(trimmed.lab_technician_raw) IN ('NULL', 'N/A', 'TBD', '-', 'SEE ATTACHED')
             THEN NULL ELSE trimmed.lab_technician_raw END AS lab_technician_token
) AS tokens
CROSS APPLY (
    SELECT
            tokens.test_date_token AS original_test_date,
            CASE
            WHEN tokens.test_date_token IS NULL THEN NULL
            ELSE REPLACE(REPLACE(tokens.test_date_token, '.', '-'), '/', '-')
        END AS test_date_dash
) AS date_norm
CROSS APPLY (
    SELECT
            TRY_CONVERT(date, tokens.test_date_token, 23) AS try_iso_dash,
            TRY_CONVERT(date, tokens.test_date_token, 111) AS try_iso_slash,
            TRY_CONVERT(date, tokens.test_date_token, 105) AS try_dmy_dash,
            TRY_CONVERT(date, tokens.test_date_token, 103) AS try_dmy_slash,
            TRY_CONVERT(date, tokens.test_date_token, 110) AS try_mdy_dash,
            TRY_CONVERT(date, tokens.test_date_token, 101) AS try_mdy_slash,
            TRY_PARSE(tokens.test_date_token AS date USING 'en-US') AS try_parse_us,
            TRY_PARSE(tokens.test_date_token AS date USING 'en-GB') AS try_parse_gb
) AS date_attempts
CROSS APPLY (
    SELECT
            COALESCE(
            date_attempts.try_iso_dash,
            date_attempts.try_iso_slash,
            date_attempts.try_dmy_dash,
            date_attempts.try_dmy_slash,
            date_attempts.try_mdy_dash,
            date_attempts.try_mdy_slash,
            date_attempts.try_parse_us,
            date_attempts.try_parse_gb
        ) AS test_date
) AS date_final
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.test_name_token IS NULL THEN NULL
            ELSE CAST(
                REPLACE(
                    REPLACE(
                        REPLACE(tokens.test_name_token, '  ', ' '),
                        '  ', ' '),
                    '  ', ' ')
            AS nvarchar(120))
        END AS test_name
) AS test_name_clean
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.reference_range_token IS NULL THEN NULL
            ELSE LTRIM(RTRIM(tokens.reference_range_token))
        END AS reference_range_trim
) AS ref_trim
CROSS APPLY (
    SELECT
            CASE
            WHEN ref_trim.reference_range_trim IS NULL THEN NULL
            WHEN ref_trim.reference_range_trim LIKE '%-%'
            THEN CAST(
                    REPLACE(
                        REPLACE(
                            REPLACE(ref_trim.reference_range_trim, ' - ', '-'),
                            '- ', '-'),
                        ' -', '-')
                 AS nvarchar(60))
            ELSE CAST(ref_trim.reference_range_trim AS nvarchar(60))
        END AS reference_range_normalized
) AS ref_norm
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.status_token IS NULL THEN NULL
            ELSE
                CASE
                    WHEN LEN(tokens.status_token) = 1 THEN UPPER(tokens.status_token)
                    ELSE CONCAT(
                        UPPER(LEFT(tokens.status_token, 1)),
                        LOWER(SUBSTRING(tokens.status_token, 2, LEN(tokens.status_token)))
                    )
                END
        END AS status
) AS status_norm
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.lab_technician_token IS NULL THEN NULL
            ELSE CAST(
                CONCAT(
                    UPPER(LEFT(tokens.lab_technician_token, 1)),
                    LOWER(SUBSTRING(tokens.lab_technician_token, 2, LEN(tokens.lab_technician_token)))
                )
            AS nvarchar(80))
        END AS lab_technician
) AS tech_norm
CROSS APPLY (
    SELECT
            TRY_CONVERT(int, tokens.lab_result_id_token) AS lab_result_id,
            TRY_CONVERT(int, tokens.admission_id_token) AS admission_id,
            date_final.test_date AS test_date,
            test_name_clean.test_name AS test_name,
            TRY_CONVERT(decimal(18,4), tokens.result_value_token) AS result_value,
            CASE WHEN tokens.unit_token IS NULL THEN NULL ELSE CAST(tokens.unit_token AS nvarchar(20)) END AS unit,
            ref_norm.reference_range_normalized AS reference_range,
            CASE WHEN status_norm.status IS NULL THEN NULL ELSE CAST(status_norm.status AS nvarchar(30)) END AS status,
            tech_norm.lab_technician
) AS typed
    WHERE typed.lab_result_id IS NOT NULL
        AND typed.admission_id IS NOT NULL;
GO
