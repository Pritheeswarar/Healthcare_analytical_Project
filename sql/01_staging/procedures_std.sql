USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.procedures_std
AS
    /*
    Staging view over dbo.procedures applying data type standardization and text cleanup only.
    Placeholder tokens collapse to NULL; TRY_CONVERT is used for resilient typing.
*/
    SELECT
        typed.procedure_id,
        typed.admission_id,
        typed.provider_id,
        typed.cpt_code,
        typed.procedure_description,
        typed.procedure_date,
        typed.procedure_cost
    FROM dbo.procedures AS pr
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), pr.procedure_id))), '') AS procedure_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), pr.admission_id))), '') AS admission_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), pr.provider_id))), '') AS provider_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(20), pr.cpt_code))), '') AS cpt_code_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(400), pr.procedure_description))), '') AS procedure_description_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(60), pr.procedure_date))), '') AS procedure_date_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), pr.procedure_cost))), '') AS procedure_cost_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE
            WHEN trimmed.procedure_id_raw IS NULL
                OR UPPER(trimmed.procedure_id_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.procedure_id_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(trimmed.procedure_id_raw, ',', '')
        END AS procedure_id_token,
            CASE
            WHEN trimmed.admission_id_raw IS NULL
                OR UPPER(trimmed.admission_id_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.admission_id_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(trimmed.admission_id_raw, ',', '')
        END AS admission_id_token,
            CASE
            WHEN trimmed.provider_id_raw IS NULL
                OR UPPER(trimmed.provider_id_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.provider_id_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(trimmed.provider_id_raw, ',', '')
        END AS provider_id_token,
            CASE
            WHEN trimmed.cpt_code_raw IS NULL
                OR UPPER(trimmed.cpt_code_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.cpt_code_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(REPLACE(trimmed.cpt_code_raw, ' ', ''), ',', '')
        END AS cpt_code_token,
            CASE
            WHEN trimmed.procedure_description_raw IS NULL
                OR UPPER(trimmed.procedure_description_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.procedure_description_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE trimmed.procedure_description_raw
        END AS procedure_description_token,
            CASE
            WHEN trimmed.procedure_date_raw IS NULL
                OR UPPER(trimmed.procedure_date_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.procedure_date_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE trimmed.procedure_date_raw
        END AS procedure_date_token,
            CASE
            WHEN trimmed.procedure_cost_raw IS NULL
                OR UPPER(trimmed.procedure_cost_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.procedure_cost_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(REPLACE(trimmed.procedure_cost_raw, ',', ''), ' ', '')
        END AS procedure_cost_token
) AS tokens
CROSS APPLY (
    SELECT TRY_CONVERT(int, tokens.procedure_id_token) AS procedure_id
) AS procedure_id_val
CROSS APPLY (
    SELECT TRY_CONVERT(int, tokens.admission_id_token) AS admission_id
) AS admission_id_val
CROSS APPLY (
    SELECT TRY_CONVERT(int, tokens.provider_id_token) AS provider_id
) AS provider_id_val
CROSS APPLY (
    SELECT TRY_CONVERT(int, tokens.cpt_code_token) AS cpt_code_int,
            TRY_CONVERT(decimal(10,5), tokens.cpt_code_token) AS cpt_code_decimal,
            CASE WHEN tokens.cpt_code_token LIKE '[0-9][0-9][0-9][0-9][0-9]' THEN tokens.cpt_code_token ELSE NULL END AS cpt_code_5_digit
) AS cpt_attempts
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.cpt_code_token IS NULL THEN NULL
            WHEN cpt_attempts.cpt_code_int IS NOT NULL
                AND cpt_attempts.cpt_code_int BETWEEN 0 AND 99999
            THEN RIGHT(CONCAT('00000', CONVERT(varchar(10), cpt_attempts.cpt_code_int)), 5)
            WHEN cpt_attempts.cpt_code_decimal IS NOT NULL
                AND ABS(cpt_attempts.cpt_code_decimal - FLOOR(cpt_attempts.cpt_code_decimal)) < 0.000001
                AND FLOOR(cpt_attempts.cpt_code_decimal) BETWEEN 0 AND 99999
            THEN RIGHT(CONCAT('00000', CONVERT(varchar(10), TRY_CONVERT(int, FLOOR(cpt_attempts.cpt_code_decimal)))), 5)
            ELSE cpt_attempts.cpt_code_5_digit
        END AS cpt_code_padded
) AS cpt_final
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.procedure_description_token IS NULL THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(tokens.procedure_description_token, '  ', ' '), '  ', ' '), '  ', ' ')
        END AS procedure_description_collapsed
) AS desc_prep
OUTER APPLY (
    SELECT
            CASE
            WHEN desc_prep.procedure_description_collapsed IS NULL THEN NULL
            ELSE desc_prep.procedure_description_collapsed
        END AS base_text,
            CASE
            WHEN desc_prep.procedure_description_collapsed IS NULL THEN 0
            ELSE LEN(desc_prep.procedure_description_collapsed)
        END AS text_length
) AS desc_info
OUTER APPLY (
    SELECT
            CASE
            WHEN desc_info.base_text IS NULL THEN NULL
            ELSE (
                SELECT STRING_AGG(
                           CASE
                               WHEN w.is_acronym = 1 THEN w.word_original
                               ELSE w.formatted_word
                           END,
                           ' '
                       ) WITHIN GROUP (ORDER BY w.word_index)
            FROM (
                    SELECT
                    ROW_NUMBER() OVER (ORDER BY pos) AS word_index,
                    word_raw AS word_original,
                    CASE
                            WHEN word_upper COLLATE Latin1_General_BIN = word_raw COLLATE Latin1_General_BIN
                        AND LEN(word_raw) BETWEEN 2 AND 4
                            THEN 1
                            ELSE 0
                        END AS is_acronym,
                    CASE
                            WHEN LEN(word_lower) = 0 THEN word_raw
                            WHEN LEN(word_lower) = 1 THEN UPPER(word_lower)
                            ELSE CONCAT(
                                    UPPER(LEFT(word_lower, 1)),
                                    SUBSTRING(word_lower, 2, LEN(word_lower) - 1)
                                 )
                        END AS formatted_word
                FROM (
                        SELECT
                        seq.pos,
                        SUBSTRING(desc_info.base_text, seq.pos, next_pos - seq.pos) AS word_raw,
                        LOWER(SUBSTRING(desc_info.base_text, seq.pos, next_pos - seq.pos)) AS word_lower,
                        UPPER(SUBSTRING(desc_info.base_text, seq.pos, next_pos - seq.pos)) AS word_upper
                    FROM (
                            SELECT TOP (CASE WHEN desc_info.text_length > 0 THEN desc_info.text_length ELSE 0 END)
                            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS pos
                        FROM (VALUES(0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0)) AS a(n)
                            CROSS JOIN (VALUES(0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0)) AS b(n)
                            CROSS JOIN (VALUES(0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0),
                                (0)) AS c(n)
                        ) AS seq
                        CROSS APPLY (
                            SELECT CHARINDEX(' ', desc_info.base_text + ' ', seq.pos) AS next_pos_candidate
                        ) AS np
                        CROSS APPLY (
                            SELECT CASE WHEN np.next_pos_candidate = 0 THEN desc_info.text_length + 1 ELSE np.next_pos_candidate END AS next_pos
                        ) AS np_final
                    WHERE (seq.pos = 1 OR SUBSTRING(desc_info.base_text, seq.pos - 1, 1) = ' ')
                        AND seq.pos <= desc_info.text_length
                        AND SUBSTRING(desc_info.base_text, seq.pos, 1) <> ' '
                    ) AS word_data
                ) AS w
            )
        END AS procedure_description_title
) AS desc_final
CROSS APPLY (
    SELECT
            CASE
            WHEN tokens.procedure_date_token IS NULL THEN NULL
            ELSE REPLACE(REPLACE(tokens.procedure_date_token, '/', '-'), '.', '-')
        END AS procedure_date_dash,
            CASE WHEN tokens.procedure_date_token IS NULL THEN 0 ELSE CASE WHEN tokens.procedure_date_token LIKE '%[A-Za-z]%' THEN 1 ELSE 0 END END AS has_alpha
) AS date_norm
OUTER APPLY (
    SELECT
            CHARINDEX('-', date_norm.procedure_date_dash) AS first_dash,
            CASE
            WHEN CHARINDEX('-', date_norm.procedure_date_dash) > 0
            THEN CHARINDEX('-', date_norm.procedure_date_dash, CHARINDEX('-', date_norm.procedure_date_dash) + 1)
            ELSE 0
        END AS second_dash
) AS dash_pos
OUTER APPLY (
    SELECT
            CASE
            WHEN date_norm.procedure_date_dash IS NULL THEN NULL
            WHEN dash_pos.first_dash > 1 AND dash_pos.second_dash > dash_pos.first_dash
            THEN TRY_CONVERT(int, LEFT(date_norm.procedure_date_dash, dash_pos.first_dash - 1))
            ELSE NULL
        END AS part1,
            CASE
            WHEN date_norm.procedure_date_dash IS NULL THEN NULL
            WHEN dash_pos.first_dash > 0 AND dash_pos.second_dash > dash_pos.first_dash
            THEN TRY_CONVERT(int,
                    SUBSTRING(
                        date_norm.procedure_date_dash,
                        dash_pos.first_dash + 1,
                        dash_pos.second_dash - dash_pos.first_dash - 1))
            ELSE NULL
        END AS part2
) AS date_parts
OUTER APPLY (
    SELECT
            CASE
            WHEN tokens.procedure_date_token IS NULL THEN NULL
            ELSE COALESCE(
                TRY_CONVERT(date, tokens.procedure_date_token, 23),
                TRY_CONVERT(date, tokens.procedure_date_token, 111),
                TRY_CONVERT(date, tokens.procedure_date_token, 112),
                CASE WHEN date_norm.has_alpha = 1 THEN TRY_PARSE(tokens.procedure_date_token AS date USING 'en-US') END,
                CASE WHEN date_norm.has_alpha = 1 THEN TRY_PARSE(tokens.procedure_date_token AS date USING 'en-GB') END,
                CASE
                    WHEN date_parts.part1 IS NOT NULL AND date_parts.part2 IS NOT NULL AND date_parts.part1 > 12 AND date_parts.part2 BETWEEN 1 AND 31
                    THEN TRY_CONVERT(date, date_norm.procedure_date_dash, 105)
                    WHEN date_parts.part1 IS NOT NULL AND date_parts.part2 IS NOT NULL AND date_parts.part2 > 12 AND date_parts.part1 BETWEEN 1 AND 12
                    THEN TRY_CONVERT(date, date_norm.procedure_date_dash, 101)
                    ELSE NULL
                END
            )
        END AS procedure_date
) AS date_final
CROSS APPLY (
    SELECT TRY_CONVERT(decimal(18,2), tokens.procedure_cost_token) AS procedure_cost
) AS cost_final
CROSS APPLY (
    SELECT
            procedure_id_val.procedure_id AS procedure_id,
            admission_id_val.admission_id AS admission_id,
            provider_id_val.provider_id AS provider_id,
            CASE WHEN cpt_final.cpt_code_padded IS NULL THEN NULL ELSE CAST(cpt_final.cpt_code_padded AS nvarchar(5)) END AS cpt_code,
            CASE WHEN desc_final.procedure_description_title IS NULL THEN NULL ELSE CAST(desc_final.procedure_description_title AS nvarchar(200)) END AS procedure_description,
            date_final.procedure_date AS procedure_date,
            cost_final.procedure_cost AS procedure_cost
) AS typed
    WHERE typed.procedure_id IS NOT NULL
        AND typed.admission_id IS NOT NULL;
GO
