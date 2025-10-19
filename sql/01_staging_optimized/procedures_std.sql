/*
    Source table: dbo.procedures
    Transformed columns: procedure_id, admission_id, provider_id, cpt_code,
        procedure_description, procedure_date, procedure_cost
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'TBD', '-', NCHAR(8212)
    Date styles used: 23, 111, 112, 101, 103, 105, 107, 100 plus heuristic day/month swaps
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.procedures_std
AS
    WITH
        source
        AS
        (
            SELECT
                procedure_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(40), pr.procedure_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                admission_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(40), pr.admission_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                provider_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(40), pr.provider_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                cpt_code_text = TRIM(TRANSLATE(CONVERT(nvarchar(20), pr.cpt_code), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                procedure_description_text = TRIM(TRANSLATE(CONVERT(nvarchar(400), pr.procedure_description), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                procedure_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(60), pr.procedure_date), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                procedure_cost_text = TRIM(TRANSLATE(CONVERT(nvarchar(40), pr.procedure_cost), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.procedures AS pr
        ),
        tokens_base
        AS
        (
            SELECT
                procedure_id_token = CASE
            WHEN procedure_id_text IS NULL OR procedure_id_text = ''
                    OR UPPER(procedure_id_text) IN ('NULL', 'N/A', 'TBD')
                    OR procedure_id_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(procedure_id_text, ',', '')
        END,
                admission_id_token = CASE
            WHEN admission_id_text IS NULL OR admission_id_text = ''
                    OR UPPER(admission_id_text) IN ('NULL', 'N/A', 'TBD')
                    OR admission_id_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(admission_id_text, ',', '')
        END,
                provider_id_token = CASE
            WHEN provider_id_text IS NULL OR provider_id_text = ''
                    OR UPPER(provider_id_text) IN ('NULL', 'N/A', 'TBD')
                    OR provider_id_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(provider_id_text, ',', '')
        END,
                cpt_code_token = CASE
            WHEN cpt_code_text IS NULL OR cpt_code_text = ''
                    OR UPPER(cpt_code_text) IN ('NULL', 'N/A', 'TBD')
                    OR cpt_code_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(REPLACE(cpt_code_text, ' ', ''), ',', '')
        END,
                procedure_description_body = CASE
        WHEN procedure_description_text IS NULL OR procedure_description_text = ''
                    OR UPPER(procedure_description_text) IN ('NULL', 'N/A', 'TBD')
                    OR procedure_description_text IN ('-', NCHAR(8212))
        THEN NULL
        ELSE REPLACE(REPLACE(REPLACE(procedure_description_text, ',', ' , '), '  ', ' '), '  ', ' ')
    END,
                procedure_date_token = CASE
            WHEN procedure_date_text IS NULL OR procedure_date_text = ''
                    OR UPPER(procedure_date_text) IN ('NULL', 'N/A', 'TBD')
                    OR procedure_date_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE procedure_date_text
        END,
                procedure_date_dash = CASE
            WHEN procedure_date_text IS NULL OR procedure_date_text = ''
                    OR UPPER(procedure_date_text) IN ('NULL', 'N/A', 'TBD')
                    OR procedure_date_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(REPLACE(procedure_date_text, '/', '-'), '.', '-')
        END,
                procedure_date_has_alpha = CASE
            WHEN procedure_date_text IS NULL THEN 0
            WHEN procedure_date_text LIKE '%[A-Za-z]%' THEN 1 ELSE 0
        END,
                procedure_cost_token = CASE
            WHEN procedure_cost_text IS NULL OR procedure_cost_text = ''
                    OR UPPER(procedure_cost_text) IN ('NULL', 'N/A', 'TBD')
                    OR procedure_cost_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(REPLACE(procedure_cost_text, ',', ''), ' ', '')
    END
            FROM source
        ),
        tokens
        AS
        (
            SELECT
                tokens_base.*,
                procedure_description_json = CASE
        WHEN tokens_base.procedure_description_body IS NULL THEN NULL
        ELSE CONCAT(
                '["',
                REPLACE(
                    STRING_ESCAPE(REPLACE(tokens_base.procedure_description_body, ' , ', '<<COMMA>>'), 'json'),
                    ' ',
                    '","'
                ),
                '"]'
            )
    END
            FROM tokens_base
        ),
        date_parts
        AS
        (
            SELECT
                tokens.*,
                first_dash = CHARINDEX('-', procedure_date_dash),
                second_dash = CASE
            WHEN CHARINDEX('-', procedure_date_dash) > 0
                THEN CHARINDEX('-', procedure_date_dash, CHARINDEX('-', procedure_date_dash) + 1)
            ELSE 0
        END,
                part1_text = CASE
            WHEN CHARINDEX('-', procedure_date_dash) > 1
            THEN LEFT(procedure_date_dash, CHARINDEX('-', procedure_date_dash) - 1)
            ELSE NULL
        END,
                part2_text = CASE
            WHEN CHARINDEX('-', procedure_date_dash) > 0
                    AND CASE
                        WHEN CHARINDEX('-', procedure_date_dash) > 0
                            THEN CHARINDEX('-', procedure_date_dash, CHARINDEX('-', procedure_date_dash) + 1)
                        ELSE 0
                    END > CHARINDEX('-', procedure_date_dash) + 1
            THEN SUBSTRING(
                    procedure_date_dash,
                    CHARINDEX('-', procedure_date_dash) + 1,
                    CASE
                        WHEN CHARINDEX('-', procedure_date_dash, CHARINDEX('-', procedure_date_dash) + 1) > 0
                            THEN CHARINDEX('-', procedure_date_dash, CHARINDEX('-', procedure_date_dash) + 1) - CHARINDEX('-', procedure_date_dash) - 1
                        ELSE 0
                    END
                )
            ELSE NULL
        END
            FROM tokens
        ),
        date_numbers
        AS
        (
            SELECT
                date_parts.*,
                part1_int = TRY_CONVERT(int, part1_text),
                part2_int = TRY_CONVERT(int, part2_text)
            FROM date_parts
        ),
        typed
        AS
        (
            SELECT
                procedure_id = TRY_CONVERT(int, dn.procedure_id_token),
                admission_id = TRY_CONVERT(int, dn.admission_id_token),
                provider_id = TRY_CONVERT(int, dn.provider_id_token),
                cpt_code = CASE
            WHEN dn.cpt_code_token IS NULL THEN NULL
            WHEN TRY_CONVERT(int, dn.cpt_code_token) IS NOT NULL
                    AND TRY_CONVERT(int, dn.cpt_code_token) BETWEEN 0 AND 99999
                THEN RIGHT('00000' + CONVERT(varchar(10), TRY_CONVERT(int, dn.cpt_code_token)), 5)
            WHEN TRY_CONVERT(decimal(10,5), dn.cpt_code_token) IS NOT NULL
                    AND ABS(TRY_CONVERT(decimal(10,5), dn.cpt_code_token) - FLOOR(TRY_CONVERT(decimal(10,5), dn.cpt_code_token))) < 0.000001
                    AND FLOOR(TRY_CONVERT(decimal(10,5), dn.cpt_code_token)) BETWEEN 0 AND 99999
                THEN RIGHT('00000' + CONVERT(varchar(10), FLOOR(TRY_CONVERT(decimal(10,5), dn.cpt_code_token))), 5)
            WHEN dn.cpt_code_token LIKE '[0-9][0-9][0-9][0-9][0-9]' THEN dn.cpt_code_token
            ELSE NULL
        END,
                procedure_description = CASE
            WHEN dn.procedure_description_json IS NULL THEN NULL
            ELSE (
                SELECT LTRIM(RTRIM(REPLACE(REPLACE(result_text, ' ,', ','), '  ', ' ')))
                FROM (
                    SELECT result_text = STUFF((
                            SELECT ' ' + CASE
                                    WHEN LEN(tokens.[value]) BETWEEN 2 AND 4
                                AND tokens.[value] COLLATE Latin1_General_BIN = UPPER(tokens.[value]) COLLATE Latin1_General_BIN
                                AND tokens.[value] NOT LIKE '%[^A-Za-z]%'
                                    THEN UPPER(tokens.[value])
                                    WHEN tokens.[value] = '<<COMMA>>' THEN ','
                                    WHEN LEN(tokens.[value]) <= 1 THEN UPPER(tokens.[value])
                                    ELSE CONCAT(
                                            UPPER(LEFT(LOWER(tokens.[value]), 1)),
                                            SUBSTRING(LOWER(tokens.[value]), 2, LEN(tokens.[value]) - 1)
                                        )
                                END
                        FROM OPENJSON(dn.procedure_description_json) AS tokens
                        ORDER BY TRY_CONVERT(int, tokens.[key])
                        FOR XML PATH(''), TYPE
                        ).value('.', 'nvarchar(max)'), 1, 1, '')
                ) AS fmt
            )
        END,
                procedure_date = COALESCE(
                    TRY_CONVERT(date, dn.procedure_date_token, 23),
                    TRY_CONVERT(date, dn.procedure_date_token, 111),
                    TRY_CONVERT(date, dn.procedure_date_token, 112),
                    TRY_CONVERT(date, dn.procedure_date_token, 101),
                    TRY_CONVERT(date, dn.procedure_date_token, 103),
                    TRY_CONVERT(date, dn.procedure_date_token, 105),
                    TRY_CONVERT(date, dn.procedure_date_token, 107),
                    TRY_CONVERT(date, dn.procedure_date_token, 100),
                    TRY_CONVERT(date, dn.procedure_date_token),
                    CASE
                        WHEN dn.procedure_date_token IS NULL THEN NULL
                        WHEN dn.part1_int IS NOT NULL AND dn.part2_int IS NOT NULL
                                AND dn.part1_int > 12 AND dn.part2_int BETWEEN 1 AND 31
                            THEN TRY_CONVERT(date, dn.procedure_date_dash, 105)
                        WHEN dn.part1_int IS NOT NULL AND dn.part2_int IS NOT NULL
                                AND dn.part2_int > 12 AND dn.part1_int BETWEEN 1 AND 12
                            THEN TRY_CONVERT(date, dn.procedure_date_dash, 101)
                        ELSE NULL
                    END
                ),
                procedure_cost = TRY_CONVERT(decimal(18, 2), dn.procedure_cost_token)
            FROM date_numbers AS dn
        )
    SELECT
        procedure_id,
        admission_id,
        provider_id,
        CASE WHEN cpt_code IS NULL THEN NULL ELSE CAST(cpt_code AS nvarchar(5)) END AS cpt_code,
        CASE WHEN procedure_description IS NULL THEN NULL ELSE CAST(procedure_description AS nvarchar(200)) END AS procedure_description,
        procedure_date,
        procedure_cost
    FROM typed
    WHERE procedure_id IS NOT NULL
        AND admission_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no CROSS APPLY-based tally expansion.
-- Confirm: CPT code padding mirrors legacy rules.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.procedures_std ORDER BY procedure_date DESC;
