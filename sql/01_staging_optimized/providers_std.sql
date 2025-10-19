/*
    Source table: dbo.providers
    Transformed columns: provider_id, department_id, name, specialty, years_experience
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'TBD', '-', NCHAR(8212)
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.providers_std
AS
    WITH
    source
    AS
    (
        SELECT
            provider_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(40), pr.provider_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
            department_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(40), pr.department_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
            name_text = TRIM(TRANSLATE(CONVERT(nvarchar(200), pr.name), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
            specialty_text = TRIM(TRANSLATE(CONVERT(nvarchar(120), pr.specialty), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
            years_experience_text = TRIM(TRANSLATE(CONVERT(nvarchar(10), pr.years_experience), CHAR(9) + CHAR(10) + CHAR(13), '   '))
        FROM dbo.providers AS pr
    ),
    tokens
    AS
    (
        SELECT
            provider_id_token = CASE
            WHEN provider_id_text IS NULL OR provider_id_text = ''
                OR UPPER(provider_id_text) IN ('NULL', 'N/A', 'TBD')
                OR provider_id_text IN ('-', NCHAR(8212))
            THEN NULL
            normalized
            AS
            (
                SELECT
                tokens.*,
                name_has_prefix = CASE
                WHEN name_token IS NULL THEN 0
                WHEN LEN(name_token) >= 2 AND UPPER(LEFT(name_token, 2)) = 'DR'
                    AND (LEN(name_token) = 2 OR SUBSTRING(name_token, 3, 1) IN ('.', ' '))
                THEN 1 ELSE 0 END,
                name_body = CASE
                WHEN name_token IS NULL THEN NULL
                WHEN LEN(name_token) >= 3 AND UPPER(LEFT(name_token, 2)) = 'DR'
                    AND SUBSTRING(name_token, 3, 1) = '.'
                THEN LTRIM(SUBSTRING(name_token, 4, 4000))
                WHEN LEN(name_token) >= 2 AND UPPER(LEFT(name_token, 2)) = 'DR'
                    AND SUBSTRING(name_token, 3, 1) = ' '
                THEN LTRIM(SUBSTRING(name_token, 3, 4000))
                ELSE name_token
            END,
                specialty_body = specialty_token
            FROM tokens
            ),
            typed
            WHEN LEN
    (name_token) >= 2 AND UPPER
(LEFT
(name_token, 2)) = 'DR'
                    AND SUBSTRING
(name_token, 3, 1) = ' '
            THEN LTRIM
(SUBSTRING
(name_token, 3, 4000))
            ELSE name_token
END,
                    name_has_prefix = normalized.name_has_prefix,
                    name_formatted = CASE
                WHEN name_body IS NULL THEN NULL
                ELSE
(
                    SELECT LTRIM(RTRIM(REPLACE(REPLACE(result_text, ' ,', ','), '  ', ' ')))
FROM (
                        SELECT STRING_AGG(
                                   CASE
                                       WHEN LEN(words.value) BETWEEN 2 AND 4
            AND words.value COLLATE Latin1_General_BIN = UPPER(words.value) COLLATE Latin1_General_BIN
                                        THEN words.value
                                       WHEN words.value = ',' THEN ','
                                       WHEN LEN(words.value) <= 1 THEN UPPER(words.value)
                                       ELSE CONCAT(
                                            UPPER(LEFT(LOWER(words.value), 1)),
                                            SUBSTRING(LOWER(words.value), 2, LEN(words.value) - 1)
                                        )
                                   END,
                                   ' '
                               ) WITHIN GROUP (ORDER BY words.ordinal) AS result_text
    FROM STRING_SPLIT(REPLACE(REPLACE(name_body, ',', ' , '), '  ', ' '), ' ', 1) AS words
    WHERE words.value <> ''
                    ) AS fmt
                )
END,
                    specialty = CASE
                WHEN specialty_body IS NULL THEN NULL
                ELSE
(
                    SELECT LTRIM(RTRIM(REPLACE(result_text, '  ', ' ')))
FROM (
                        SELECT STRING_AGG(
                                   CASE
                                       WHEN LEN(words.value) BETWEEN 2 AND 4
            AND words.value COLLATE Latin1_General_BIN = UPPER(words.value) COLLATE Latin1_General_BIN
            AND words.value NOT LIKE '%[^A-Za-z]%'
                                        THEN UPPER(words.value)
                                       WHEN LEN(words.value) <= 1 THEN UPPER(words.value)
                                       ELSE CONCAT(
                                            UPPER(LEFT(LOWER(words.value), 1)),
                                            SUBSTRING(LOWER(words.value), 2, LEN(words.value) - 1)
                                        )
                                   END,
                                   ' '
                               ) WITHIN GROUP (ORDER BY words.ordinal) AS result_text
    FROM STRING_SPLIT(REPLACE(REPLACE(LOWER(specialty_body), ',', ' , '), '  ', ' '), ' ', 1) AS words
    WHERE words.value <> ''
                    ) AS fmt
                )
END,
                       ) WITHIN GROUP
(ORDER BY TRY_CONVERT
(int, j.[key]))
                FROM OPENJSON
(name_json) AS j
            )
END,
                specialty_formatted = CASE
                FROM normalized
            ELSE
(
                SELECT STRING_AGG(
                           CASE
                               WHEN LEN(j.[value]) BETWEEN 2 AND 4
        AND j.[value] COLLATE Latin1_General_BIN = UPPER(j.[value]) COLLATE Latin1_General_BIN
        AND j.[value] NOT LIKE '%[^A-Za-z]%'
                                   THEN UPPER(j.[value])
                               WHEN LEN(j.[value]) <= 1 THEN UPPER(j.[value])
                               ELSE CONCAT(UPPER(LEFT(LOWER(j.[value]), 1)), SUBSTRING(LOWER(j.[value]), 2, LEN(j.[value]) - 1))
                           END,
                           ' '
                       ) WITHIN GROUP (ORDER BY TRY_CONVERT(int, j.[key]))
FROM OPENJSON(specialty_json) AS j
            )
END,
                years_experience = CASE
            WHEN TRY_CONVERT
(tinyint, years_experience_token) BETWEEN 0 AND 60 THEN TRY_CONVERT
(tinyint, years_experience_token)
            ELSE NULL
END
            FROM split_tokens
        ),
        final
        AS
(
            SELECT
    provider_id,
    department_id,
    name = CASE
            WHEN name_formatted IS NULL THEN NULL
            WHEN name_has_prefix = 1 THEN CONCAT('Dr. ', name_formatted)
            ELSE name_formatted
        END,
    specialty = specialty_formatted,
    years_experience
FROM typed
        )
SELECT
    provider_id,
    department_id,
    CASE WHEN name IS NULL THEN NULL ELSE CAST(name AS nvarchar(120)) END AS name,
    CASE WHEN specialty IS NULL THEN NULL ELSE CAST(specialty AS nvarchar(80)) END AS specialty,
    years_experience
FROM final
WHERE provider_id IS NOT NULL
    AND department_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no APPLY-based tally expansions.
-- Confirm: provider and department identifiers remain required.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.providers_std ORDER BY provider_id;
