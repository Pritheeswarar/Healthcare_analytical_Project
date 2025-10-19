USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.diagnoses_std
AS
    /*
    Staging view over dbo.diagnoses applying datatype standardization only.
    Placeholder tokens collapse to NULL; TRY_CONVERT / TRY_PARSE enforce resilient typing.
*/
    SELECT
        typed.diagnosis_id,
        typed.admission_id,
        typed.diagnosis_date,
        typed.icd_code,
        typed.diagnosis_type,
        typed.diagnosis_description
    FROM dbo.diagnoses AS d
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.diagnosis_id))), '') AS diagnosis_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.admission_id))), '') AS admission_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.diagnosis_date))), '') AS diagnosis_date_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.icd_code))), '') AS icd_code_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.diagnosis_type))), '') AS diagnosis_type_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), d.diagnosis_description))), '') AS diagnosis_description_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE WHEN trimmed.diagnosis_id_raw IS NULL OR UPPER(trimmed.diagnosis_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.diagnosis_id_raw, ',', '') END AS diagnosis_id_token,
            CASE WHEN trimmed.admission_id_raw IS NULL OR UPPER(trimmed.admission_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.admission_id_raw, ',', '') END AS admission_id_token,
            CASE WHEN trimmed.diagnosis_date_raw IS NULL OR UPPER(trimmed.diagnosis_date_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.diagnosis_date_raw END AS diagnosis_date_token,
            CASE WHEN trimmed.icd_code_raw IS NULL OR UPPER(trimmed.icd_code_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.icd_code_raw, ' ', '') END AS icd_code_token,
            CASE WHEN trimmed.diagnosis_type_raw IS NULL OR UPPER(trimmed.diagnosis_type_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.diagnosis_type_raw END AS diagnosis_type_token,
            CASE WHEN trimmed.diagnosis_description_raw IS NULL OR UPPER(trimmed.diagnosis_description_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.diagnosis_description_raw END AS diagnosis_description_token
) AS tokens
CROSS APPLY (
    SELECT
            tokens.diagnosis_date_token AS diagnosis_date_original,
            CASE
            WHEN tokens.diagnosis_date_token IS NULL THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(tokens.diagnosis_date_token, '/', '-'), '.', '-'), ' ', '')
        END AS diagnosis_date_dash
) AS date_norm
CROSS APPLY (
    SELECT
            TRY_CONVERT(date, LTRIM(RTRIM(tokens.diagnosis_date_token)), 23) AS try_iso_dash,
            TRY_CONVERT(date, LTRIM(RTRIM(tokens.diagnosis_date_token)), 111) AS try_iso_slash,
            TRY_CONVERT(date, LTRIM(RTRIM(tokens.diagnosis_date_token)), 120) AS try_iso_time,
            TRY_CONVERT(date, LTRIM(RTRIM(tokens.diagnosis_date_token)), 105) AS try_dmy_dash,
            TRY_CONVERT(date, LTRIM(RTRIM(tokens.diagnosis_date_token)), 103) AS try_dmy_slash,
            TRY_CONVERT(date, LTRIM(RTRIM(tokens.diagnosis_date_token)), 110) AS try_mdy_dash,
            TRY_CONVERT(date, LTRIM(RTRIM(tokens.diagnosis_date_token)), 101) AS try_mdy_slash
) AS date_attempts
CROSS APPLY (
    SELECT
            date_norm.diagnosis_date_dash AS normalized_date_token,
            CHARINDEX('-', date_norm.diagnosis_date_dash) AS dash1,
            CASE
            WHEN date_norm.diagnosis_date_dash IS NULL THEN 0
            ELSE CHARINDEX('-', date_norm.diagnosis_date_dash, CHARINDEX('-', date_norm.diagnosis_date_dash) + 1)
        END AS dash2
) AS dash_positions
CROSS APPLY (
    SELECT
            CASE WHEN dash_positions.dash1 > 1 THEN SUBSTRING(dash_positions.normalized_date_token, 1, dash_positions.dash1 - 1) ELSE NULL END AS part1,
            CASE WHEN dash_positions.dash1 > 0 AND dash_positions.dash2 > dash_positions.dash1 + 1
             THEN SUBSTRING(dash_positions.normalized_date_token, dash_positions.dash1 + 1, dash_positions.dash2 - dash_positions.dash1 - 1)
             ELSE NULL END AS part2,
            CASE WHEN dash_positions.dash2 > 0 AND dash_positions.dash2 < LEN(dash_positions.normalized_date_token)
             THEN SUBSTRING(dash_positions.normalized_date_token, dash_positions.dash2 + 1, LEN(dash_positions.normalized_date_token) - dash_positions.dash2)
             ELSE NULL END AS part3
) AS date_parts
CROSS APPLY (
    SELECT
            TRY_CONVERT(int, date_parts.part1) AS part1_int,
            TRY_CONVERT(int, date_parts.part2) AS part2_int,
            TRY_CONVERT(int, date_parts.part3) AS part3_int,
            CASE WHEN date_parts.part3 IS NULL THEN 0 ELSE LEN(date_parts.part3) END AS part3_len
) AS date_numbers
CROSS APPLY (
    SELECT
            CASE
            WHEN date_numbers.part3_len = 2
                AND date_numbers.part1_int IS NOT NULL
                AND date_numbers.part2_int IS NOT NULL
                AND date_numbers.part3_int IS NOT NULL
            THEN TRY_CONVERT(
                    date,
                    CONCAT(
                        CASE WHEN date_numbers.part3_int BETWEEN 0 AND 29 THEN 2000 + date_numbers.part3_int ELSE 1900 + date_numbers.part3_int END,
                        '-',
                        RIGHT(CONCAT('0', CAST(date_numbers.part2_int AS varchar(2))), 2),
                        '-',
                        RIGHT(CONCAT('0', CAST(date_numbers.part1_int AS varchar(2))), 2)
                    ),
                    23)
            ELSE NULL
        END AS two_digit_dmy,
            CASE
            WHEN date_numbers.part3_len = 2
                AND date_numbers.part1_int IS NOT NULL
                AND date_numbers.part2_int IS NOT NULL
                AND date_numbers.part3_int IS NOT NULL
            THEN TRY_CONVERT(
                    date,
                    CONCAT(
                        CASE WHEN date_numbers.part3_int BETWEEN 0 AND 29 THEN 2000 + date_numbers.part3_int ELSE 1900 + date_numbers.part3_int END,
                        '-',
                        RIGHT(CONCAT('0', CAST(date_numbers.part1_int AS varchar(2))), 2),
                        '-',
                        RIGHT(CONCAT('0', CAST(date_numbers.part2_int AS varchar(2))), 2)
                    ),
                    23)
            ELSE NULL
        END AS two_digit_mdy
) AS date_two_digit
CROSS APPLY (
    SELECT
            COALESCE(
            date_attempts.try_iso_dash,
            date_attempts.try_iso_slash,
            date_attempts.try_iso_time,
            date_attempts.try_dmy_dash,
            date_attempts.try_dmy_slash,
            date_attempts.try_mdy_dash,
            date_attempts.try_mdy_slash,
            date_two_digit.two_digit_dmy,
            date_two_digit.two_digit_mdy
        ) AS diagnosis_date
) AS date_final
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.icd_code_token IS NULL THEN NULL
            ELSE UPPER(tokens.icd_code_token)
        END AS icd_code_upper
) AS icd_norm
CROSS APPLY (
    SELECT
            CASE
            WHEN icd_norm.icd_code_upper IS NULL THEN NULL
            WHEN PATINDEX('%[^A-Z0-9.]%', icd_norm.icd_code_upper) <> 0 THEN NULL
            WHEN CHARINDEX('.', icd_norm.icd_code_upper) = 0
                AND LEN(icd_norm.icd_code_upper) BETWEEN 4 AND 7
                AND icd_norm.icd_code_upper LIKE '[A-Z][0-9][0-9][A-Z0-9]%'
            THEN CAST(icd_norm.icd_code_upper AS nvarchar(10))
            WHEN CHARINDEX('.', icd_norm.icd_code_upper) = 4
                AND LEN(icd_norm.icd_code_upper) BETWEEN 5 AND 8
                AND REPLACE(icd_norm.icd_code_upper, '.', '') LIKE '[A-Z][0-9][0-9][A-Z0-9]%'
                AND LEN(REPLACE(icd_norm.icd_code_upper, '.', '')) BETWEEN 4 AND 7
            THEN CAST(icd_norm.icd_code_upper AS nvarchar(10))
            ELSE NULL
        END AS icd_code
) AS icd_final
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.diagnosis_type_token IS NULL THEN NULL
            ELSE CASE UPPER(tokens.diagnosis_type_token)
                    WHEN 'PRIMARY' THEN N'Primary'
                    WHEN 'SECONDARY' THEN N'Secondary'
                    ELSE NULL
                 END
        END AS diagnosis_type
) AS type_final
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.diagnosis_description_token IS NULL THEN NULL
            ELSE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(tokens.diagnosis_description_token, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' ')))
        END AS description_stage1
) AS desc_stage1
CROSS APPLY (
    SELECT
            CASE
            WHEN desc_stage1.description_stage1 IS NULL THEN NULL
            ELSE NULLIF(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(desc_stage1.description_stage1, '  ', ' '),
                                '  ', ' '),
                            '  ', ' '),
                        '  ', ' '),
                    '  ', ' '),
                '')
        END AS diagnosis_description
) AS desc_final
CROSS APPLY (
    SELECT
            TRY_CONVERT(int, tokens.diagnosis_id_token) AS diagnosis_id,
            TRY_CONVERT(int, tokens.admission_id_token) AS admission_id,
            date_final.diagnosis_date,
            icd_final.icd_code,
            type_final.diagnosis_type,
            CASE
            WHEN desc_final.diagnosis_description IS NULL THEN NULL
            ELSE CAST(desc_final.diagnosis_description AS nvarchar(255))
        END AS diagnosis_description
) AS typed
    WHERE typed.diagnosis_id IS NOT NULL
        AND typed.admission_id IS NOT NULL;
GO
