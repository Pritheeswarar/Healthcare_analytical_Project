USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.providers_std
AS
    /*
    Staging view over dbo.providers performing standardization only.
    Identifiers are typed with TRY_CONVERT; text fields are normalized without introducing business logic.
*/
    WITH
        tally
        AS
        (
            SELECT TOP (1000)
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
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
        )
    SELECT
        typed.provider_id,
        typed.department_id,
        typed.name,
        typed.specialty,
        typed.years_experience
    FROM dbo.providers AS pr
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), pr.provider_id))), '') AS provider_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), pr.department_id))), '') AS department_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(200), pr.name))), '') AS name_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(120), pr.specialty))), '') AS specialty_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(10), pr.years_experience))), '') AS years_experience_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE
            WHEN trimmed.provider_id_raw IS NULL
                OR UPPER(trimmed.provider_id_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.provider_id_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(trimmed.provider_id_raw, ',', '')
        END AS provider_id_token,
            CASE
            WHEN trimmed.department_id_raw IS NULL
                OR UPPER(trimmed.department_id_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.department_id_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(trimmed.department_id_raw, ',', '')
        END AS department_id_token,
            CASE
            WHEN trimmed.name_raw IS NULL
                OR UPPER(trimmed.name_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.name_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE trimmed.name_raw
        END AS name_token,
            CASE
            WHEN trimmed.specialty_raw IS NULL
                OR UPPER(trimmed.specialty_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.specialty_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE trimmed.specialty_raw
        END AS specialty_token,
            CASE
            WHEN trimmed.years_experience_raw IS NULL
                OR UPPER(trimmed.years_experience_raw) IN ('NULL', 'N/A', 'TBD')
                OR trimmed.years_experience_raw IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(trimmed.years_experience_raw, ',', '')
        END AS years_experience_token
) AS tokens
CROSS APPLY (
    SELECT TRY_CONVERT(int, tokens.provider_id_token) AS provider_id
) AS provider_id_val
CROSS APPLY (
    SELECT TRY_CONVERT(int, tokens.department_id_token) AS department_id
) AS department_id_val
CROSS APPLY (
    SELECT TRY_CONVERT(tinyint, tokens.years_experience_token) AS years_experience_raw
) AS years_experience_val
CROSS APPLY (
    SELECT
            CASE
            WHEN years_experience_val.years_experience_raw IS NULL THEN NULL
            WHEN years_experience_val.years_experience_raw BETWEEN 0 AND 60 THEN years_experience_val.years_experience_raw
            ELSE NULL
        END AS years_experience
) AS years_experience_norm
OUTER APPLY (
    SELECT
            CASE
            WHEN tokens.name_token IS NULL THEN NULL
            ELSE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(tokens.name_token, '  ', ' '), '  ', ' '), '  ', ' ')))
        END AS name_collapsed
) AS name_clean
OUTER APPLY (
    SELECT
            CASE WHEN name_clean.name_collapsed IS NULL THEN NULL ELSE name_clean.name_collapsed END AS base_text,
            CASE WHEN name_clean.name_collapsed IS NULL THEN 0 ELSE LEN(name_clean.name_collapsed) END AS base_length
) AS name_base
OUTER APPLY (
    SELECT
            CASE
            WHEN name_base.base_length >= 2
                AND UPPER(LEFT(name_base.base_text, 2)) = 'DR'
                AND (
                        name_base.base_length = 2
                OR SUBSTRING(name_base.base_text, 3, 1) IN ('.', ' ')
                OR (name_base.base_length >= 3 AND SUBSTRING(name_base.base_text, 3, 1) = '.')
                    )
            THEN 1
            ELSE 0
        END AS has_dr_prefix,
            CASE
            WHEN name_base.base_text IS NULL THEN NULL
            WHEN name_base.base_length >= 2
                AND UPPER(LEFT(name_base.base_text, 2)) = 'DR'
                AND (
                        name_base.base_length = 2
                OR SUBSTRING(name_base.base_text, 3, 1) IN ('.', ' ')
                OR (name_base.base_length >= 3 AND SUBSTRING(name_base.base_text, 3, 1) = '.')
                    )
            THEN LTRIM(SUBSTRING(name_base.base_text,
                                CASE WHEN name_base.base_length >= 3 AND SUBSTRING(name_base.base_text, 3, 1) = '.' THEN 4 ELSE 3 END,
                                4000))
            ELSE name_base.base_text
        END AS name_without_prefix
) AS name_prefix
OUTER APPLY (
    SELECT
            CASE
            WHEN name_prefix.name_without_prefix IS NULL THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(REPLACE(name_prefix.name_without_prefix, ',', ' , '), '  ', ' '), '  ', ' '), '  ', ' ')
        END AS spaced_text
) AS name_spaced
OUTER APPLY (
    SELECT
            CASE WHEN name_spaced.spaced_text IS NULL THEN NULL ELSE name_spaced.spaced_text END AS base_text,
            CASE WHEN name_spaced.spaced_text IS NULL THEN 0 ELSE LEN(name_spaced.spaced_text) END AS text_length
) AS name_text
OUTER APPLY (
    SELECT
            CASE
            WHEN name_text.text_length <= 0 THEN NULL
            ELSE (
                SELECT STRING_AGG(token_fmt.formatted_token, ' ') WITHIN GROUP (ORDER BY token_fmt.pos)
            FROM (
                        SELECT
                    seq.pos,
                    SUBSTRING(name_text.base_text, seq.pos, next_pos - seq.pos) AS token_raw
                FROM (
                                SELECT TOP (CASE WHEN name_text.text_length > 0 THEN name_text.text_length ELSE 0 END)
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS pos
                    FROM tally
                            ) AS seq
                        CROSS APPLY (
                            SELECT CHARINDEX(' ', name_text.base_text + ' ', seq.pos) AS next_pos_candidate
                        ) AS np
                        CROSS APPLY (
                            SELECT CASE WHEN np.next_pos_candidate = 0 THEN name_text.text_length + 1 ELSE np.next_pos_candidate END AS next_pos
                        ) AS np_final
                WHERE (seq.pos = 1 OR SUBSTRING(name_text.base_text, seq.pos - 1, 1) = ' ')
                    AND seq.pos <= name_text.text_length
                    AND SUBSTRING(name_text.base_text, seq.pos, 1) <> ' '
                ) AS token_words
                OUTER APPLY (
                    SELECT
                    CASE
                                WHEN token_words.token_raw = ',' THEN ','
                                WHEN UPPER(token_words.token_raw) IN ('MD', 'MBBS', 'DO', 'DNB', 'DM', 'MCH')
                                    THEN UPPER(token_words.token_raw)
                                ELSE (
                                        SELECT STRING_AGG(
                                                   CASE
                                                       WHEN char_data.is_letter = 1
                            AND (char_data.pos = 1 OR char_data.prev_char IN ('-', NCHAR(39)))
                                                           THEN UPPER(char_data.lower_char)
                                                       WHEN char_data.is_letter = 1 THEN char_data.lower_char
                                                       ELSE char_data.original_char
                                                   END,
                                                   ''
                                               ) WITHIN GROUP (ORDER BY char_data.pos)
                    FROM (
                                                SELECT
                            num.n AS pos,
                            SUBSTRING(token_words.token_raw, num.n, 1) AS original_char,
                            LOWER(SUBSTRING(token_words.token_raw, num.n, 1)) AS lower_char,
                            CASE WHEN SUBSTRING(token_words.token_raw, num.n, 1) LIKE '[A-Za-z]' THEN 1 ELSE 0 END AS is_letter,
                            CASE WHEN num.n = 1 THEN '' ELSE SUBSTRING(token_words.token_raw, num.n - 1, 1) END AS prev_char
                        FROM tally AS num
                        WHERE num.n <= LEN(token_words.token_raw)
                                            ) AS char_data
                                    )
                            END AS formatted_token,
                    token_words.pos
                ) AS token_fmt
            )
        END AS name_body_formatted
) AS name_formatted
OUTER APPLY (
    SELECT
            CASE
            WHEN name_formatted.name_body_formatted IS NULL THEN NULL
            ELSE LTRIM(RTRIM(REPLACE(REPLACE(name_formatted.name_body_formatted, ' , ', ', '), '  ', ' ')))
        END AS name_body_final
) AS name_final
OUTER APPLY (
    SELECT
            CASE
            WHEN name_final.name_body_final IS NULL THEN NULL
            WHEN name_prefix.has_dr_prefix = 1 THEN 'Dr. ' + name_final.name_body_final
            ELSE name_final.name_body_final
        END AS name_with_prefix
) AS name_with_prefix
OUTER APPLY (
    SELECT
            CASE
            WHEN name_with_prefix.name_with_prefix IS NULL THEN NULL
            WHEN name_with_prefix.name_with_prefix = '' THEN NULL
            ELSE name_with_prefix.name_with_prefix
        END AS name_standardized
) AS name_standard
OUTER APPLY (
    SELECT
            CASE
            WHEN tokens.specialty_token IS NULL THEN NULL
            ELSE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(tokens.specialty_token, '  ', ' '), '  ', ' '), '  ', ' ')))
        END AS specialty_collapsed
) AS specialty_clean
CROSS APPLY (
    SELECT
            CASE WHEN specialty_clean.specialty_collapsed IS NULL THEN NULL ELSE LOWER(specialty_clean.specialty_collapsed) END AS specialty_lower,
            CASE WHEN specialty_clean.specialty_collapsed IS NULL THEN 0 ELSE LEN(specialty_clean.specialty_collapsed) END AS specialty_len
) AS specialty_lower_info
OUTER APPLY (
    SELECT
            CASE
            WHEN specialty_lower_info.specialty_len <= 0 THEN NULL
            ELSE (
        SELECT STRING_AGG(word_fmt.formatted_word, ' ') WITHIN GROUP (ORDER BY word_fmt.pos)
            FROM (
                        SELECT
                    seq.pos,
                    SUBSTRING(specialty_lower_info.specialty_lower, seq.pos, next_pos - seq.pos) AS word_raw
                FROM (
                                SELECT TOP (CASE WHEN specialty_lower_info.specialty_len > 0 THEN specialty_lower_info.specialty_len ELSE 0 END)
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS pos
                    FROM tally
                            ) AS seq
                        CROSS APPLY (
                            SELECT CHARINDEX(' ', specialty_lower_info.specialty_lower + ' ', seq.pos) AS next_pos_candidate
                        ) AS np
                        CROSS APPLY (
                            SELECT CASE WHEN np.next_pos_candidate = 0 THEN specialty_lower_info.specialty_len + 1 ELSE np.next_pos_candidate END AS next_pos
                        ) AS np_final
                WHERE (seq.pos = 1 OR SUBSTRING(specialty_lower_info.specialty_lower, seq.pos - 1, 1) = ' ')
                    AND seq.pos <= specialty_lower_info.specialty_len
                    AND SUBSTRING(specialty_lower_info.specialty_lower, seq.pos, 1) <> ' '
                ) AS word_data
                OUTER APPLY (
                    SELECT
                    CASE
                            WHEN word_data.word_raw = '' THEN ''
                            ELSE (
                                    SELECT STRING_AGG(
                                               CASE
                                                   WHEN char_data.is_letter = 1
                            AND (char_data.pos = 1 OR char_data.prev_char IN ('-', NCHAR(39)))
                                                   THEN UPPER(char_data.lower_char)
                                                   WHEN char_data.is_letter = 1 THEN char_data.lower_char
                                                   ELSE char_data.original_char
                                               END,
                                               ''
                                           ) WITHIN GROUP (ORDER BY char_data.pos)
                    FROM (
                                            SELECT
                            num.n AS pos,
                            SUBSTRING(word_data.word_raw, num.n, 1) AS original_char,
                            LOWER(SUBSTRING(word_data.word_raw, num.n, 1)) AS lower_char,
                            CASE WHEN SUBSTRING(word_data.word_raw, num.n, 1) LIKE '[A-Za-z]' THEN 1 ELSE 0 END AS is_letter,
                            CASE WHEN num.n = 1 THEN '' ELSE SUBSTRING(word_data.word_raw, num.n - 1, 1) END AS prev_char,
                            UPPER(SUBSTRING(word_data.word_raw, num.n, 1)) AS upper_char
                        FROM tally AS num
                        WHERE num.n <= LEN(word_data.word_raw)
                                        ) AS char_data
                            )
                        END AS base_formatted
                ) AS word_base
                CROSS APPLY (
                    SELECT
                    CASE
                            WHEN UPPER(word_data.word_raw) COLLATE Latin1_General_BIN = word_data.word_raw COLLATE Latin1_General_BIN
                        AND LEN(word_data.word_raw) BETWEEN 2 AND 4
                        AND word_data.word_raw NOT LIKE '%[^A-Za-z]%'
                                THEN UPPER(word_data.word_raw)
                            ELSE word_base.base_formatted
                        END AS formatted_word,
                    word_data.pos
                ) AS word_fmt
            )
        END AS specialty_title_case
) AS specialty_title
CROSS APPLY (
    SELECT
            provider_id_val.provider_id AS provider_id,
            department_id_val.department_id AS department_id,
            CASE WHEN name_standard.name_standardized IS NULL THEN NULL ELSE CAST(name_standard.name_standardized AS nvarchar(120)) END AS name,
            CASE WHEN specialty_title.specialty_title_case IS NULL THEN NULL ELSE CAST(specialty_title.specialty_title_case AS nvarchar(80)) END AS specialty,
            years_experience_norm.years_experience AS years_experience
) AS typed
    WHERE typed.provider_id IS NOT NULL
        AND typed.department_id IS NOT NULL;
GO
