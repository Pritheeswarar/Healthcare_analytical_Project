/*
    Source table: dbo.diagnoses
    Transformed columns: diagnosis_id, admission_id, diagnosis_date, icd_code,
        diagnosis_type, diagnosis_description
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'TBD', '-'
    Date styles used: 23, 111, 120, 105, 103, 110, 101 plus custom two-digit year handling
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.diagnoses_std
AS
    WITH
        source
        AS
        (
            SELECT
                diagnosis_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.diagnosis_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                admission_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.admission_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                diagnosis_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.diagnosis_date), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                icd_code_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.icd_code), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                diagnosis_type_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.diagnosis_type), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                diagnosis_description_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), d.diagnosis_description), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.diagnoses AS d
        ),
        tokens
        AS
        (
            SELECT
                diagnosis_id_token = CASE
            WHEN diagnosis_id_text IS NULL OR diagnosis_id_text = ''
                    OR UPPER(diagnosis_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(diagnosis_id_text, ',', '')
        END,
                admission_id_token = CASE
            WHEN admission_id_text IS NULL OR admission_id_text = ''
                    OR UPPER(admission_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(admission_id_text, ',', '')
        END,
                diagnosis_date_token = CASE
            WHEN diagnosis_date_text IS NULL OR diagnosis_date_text = ''
                    OR UPPER(diagnosis_date_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE diagnosis_date_text
        END,
                diagnosis_date_dash = CASE
            WHEN diagnosis_date_text IS NULL OR diagnosis_date_text = ''
                    OR UPPER(diagnosis_date_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(diagnosis_date_text, '/', '-'), '.', '-'), ' ', '')
        END,
                icd_code_token = CASE
            WHEN icd_code_text IS NULL OR icd_code_text = ''
                    OR UPPER(icd_code_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(icd_code_text, ' ', '')
        END,
                diagnosis_type_token = CASE
            WHEN diagnosis_type_text IS NULL OR diagnosis_type_text = ''
                    OR UPPER(diagnosis_type_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE diagnosis_type_text
        END,
                diagnosis_description_token = CASE
            WHEN diagnosis_description_text IS NULL OR diagnosis_description_text = ''
                    OR UPPER(diagnosis_description_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE diagnosis_description_text
        END
            FROM source
        ),
        date_parts
        AS
        (
            SELECT
                tokens.*,
                dash1 = CHARINDEX('-', diagnosis_date_dash),
                dash2 = CASE
            WHEN CHARINDEX('-', diagnosis_date_dash) > 0
                THEN CHARINDEX('-', diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) + 1)
            ELSE 0
        END,
                part1 = CASE
            WHEN CHARINDEX('-', diagnosis_date_dash) > 1
            THEN LEFT(diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) - 1)
            ELSE NULL
        END,
                part2 = CASE
            WHEN CHARINDEX('-', diagnosis_date_dash) > 0
                    AND CASE
                        WHEN CHARINDEX('-', diagnosis_date_dash) > 0
                            THEN CHARINDEX('-', diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) + 1)
                        ELSE 0
                    END > CHARINDEX('-', diagnosis_date_dash) + 1
            THEN SUBSTRING(
                    diagnosis_date_dash,
                    CHARINDEX('-', diagnosis_date_dash) + 1,
                    CASE
                        WHEN CHARINDEX('-', diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) + 1) > 0
                            THEN CHARINDEX('-', diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) + 1) - CHARINDEX('-', diagnosis_date_dash) - 1
                        ELSE 0
                    END
                )
            ELSE NULL
        END,
                part3 = CASE
            WHEN CASE
                    WHEN CHARINDEX('-', diagnosis_date_dash) > 0
                        THEN CHARINDEX('-', diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) + 1)
                    ELSE 0
                END > 0
                    AND CASE
                    WHEN CHARINDEX('-', diagnosis_date_dash) > 0
                        THEN CHARINDEX('-', diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) + 1)
                    ELSE 0
                END < LEN(diagnosis_date_dash)
            THEN RIGHT(diagnosis_date_dash,
                    LEN(diagnosis_date_dash) - CHARINDEX('-', diagnosis_date_dash, CHARINDEX('-', diagnosis_date_dash) + 1))
            ELSE NULL
        END
            FROM tokens
        ),
        date_numbers
        AS
        (
            SELECT
                date_parts.*,
                part1_int = TRY_CONVERT(int, part1),
                part2_int = TRY_CONVERT(int, part2),
                part3_int = TRY_CONVERT(int, part3),
                part3_len = CASE WHEN part3 IS NULL THEN 0 ELSE LEN(part3) END
            FROM date_parts
        ),
        date_adjusted
        AS
        (
            SELECT
                date_numbers.*,
                two_digit_dmy = CASE
            WHEN part3_len = 2
                    AND part1_int IS NOT NULL
                    AND part2_int IS NOT NULL
                    AND part3_int IS NOT NULL
            THEN TRY_CONVERT(date, CONCAT(
                    CASE WHEN part3_int BETWEEN 0 AND 29 THEN 2000 + part3_int ELSE 1900 + part3_int END,
                    '-', RIGHT('0' + CONVERT(varchar(2), part2_int), 2),
                    '-', RIGHT('0' + CONVERT(varchar(2), part1_int), 2)
                ), 23)
            ELSE NULL
        END,
                two_digit_mdy = CASE
            WHEN part3_len = 2
                    AND part1_int IS NOT NULL
                    AND part2_int IS NOT NULL
                    AND part3_int IS NOT NULL
            THEN TRY_CONVERT(date, CONCAT(
                    CASE WHEN part3_int BETWEEN 0 AND 29 THEN 2000 + part3_int ELSE 1900 + part3_int END,
                    '-', RIGHT('0' + CONVERT(varchar(2), part1_int), 2),
                    '-', RIGHT('0' + CONVERT(varchar(2), part2_int), 2)
                ), 23)
            ELSE NULL
        END
            FROM date_numbers
        ),
        typed
        AS
        (
            SELECT
                diagnosis_id = TRY_CONVERT(int, diagnosis_id_token),
                admission_id = TRY_CONVERT(int, admission_id_token),
                diagnosis_date = COALESCE(
            TRY_CONVERT(date, diagnosis_date_token, 23),
            TRY_CONVERT(date, diagnosis_date_token, 111),
            TRY_CONVERT(date, diagnosis_date_token, 120),
            TRY_CONVERT(date, diagnosis_date_token, 105),
            TRY_CONVERT(date, diagnosis_date_token, 103),
            TRY_CONVERT(date, diagnosis_date_token, 110),
            TRY_CONVERT(date, diagnosis_date_token, 101),
            two_digit_dmy,
            two_digit_mdy
        ),
                icd_code = CASE
            WHEN icd_code_token IS NULL THEN NULL
            ELSE CASE
                WHEN PATINDEX('%[^A-Za-z0-9.]%', UPPER(icd_code_token)) <> 0 THEN NULL
                WHEN CHARINDEX('.', UPPER(icd_code_token)) = 0
                    AND LEN(UPPER(icd_code_token)) BETWEEN 4 AND 7
                    AND UPPER(icd_code_token) LIKE '[A-Z][0-9][0-9][A-Z0-9]%'
                    THEN UPPER(icd_code_token)
                WHEN CHARINDEX('.', UPPER(icd_code_token)) = 4
                    AND LEN(UPPER(icd_code_token)) BETWEEN 5 AND 8
                    AND REPLACE(UPPER(icd_code_token), '.', '') LIKE '[A-Z][0-9][0-9][A-Z0-9]%'
                    AND LEN(REPLACE(UPPER(icd_code_token), '.', '')) BETWEEN 4 AND 7
                    THEN UPPER(icd_code_token)
                ELSE NULL
            END
        END,
                diagnosis_type = CASE
            WHEN diagnosis_type_token IS NULL THEN NULL
            ELSE CASE UPPER(diagnosis_type_token)
                WHEN 'PRIMARY' THEN N'Primary'
                WHEN 'SECONDARY' THEN N'Secondary'
                ELSE NULL
            END
        END,
                diagnosis_description = CASE
            WHEN diagnosis_description_token IS NULL THEN NULL
            ELSE NULLIF(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(diagnosis_description_token, CHAR(13), ' '),
                                CHAR(10), ' '),
                            CHAR(9), ' '),
                        '  ', ' '),
                    '  ', ' '),
                ''
            )
        END
            FROM date_adjusted
        )
    SELECT
        diagnosis_id,
        admission_id,
        diagnosis_date,
        icd_code,
        diagnosis_type,
        CASE
        WHEN diagnosis_description IS NULL THEN NULL
        ELSE CAST(diagnosis_description AS nvarchar(255))
    END AS diagnosis_description
    FROM typed
    WHERE diagnosis_id IS NOT NULL
        AND admission_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no FORMAT, no CROSS APPLY.
-- Confirm: ICD code pattern rules align with legacy logic.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.diagnoses_std ORDER BY diagnosis_date DESC;
