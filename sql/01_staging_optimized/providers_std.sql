/*
    Source table: dbo.providers
    Transformed columns: provider_id, department_id, name, specialty, years_experience
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'NA', 'TBD', 'UNKNOWN', '-', NCHAR(8212)
*/
USE [Healthcare];
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.schemas
    WHERE name = 'stg_optimized'
)
    EXEC ('CREATE SCHEMA stg_optimized');
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
                specialty_text = TRIM(TRANSLATE(CONVERT(nvarchar(200), pr.specialty), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                years_experience_text = TRIM(TRANSLATE(CONVERT(nvarchar(20), pr.years_experience), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.providers AS pr
        ),
        tokens
        AS
        (
            SELECT
                provider_id_token = CASE
                                        WHEN provider_id_text IS NULL OR provider_id_text = ''
                                            OR UPPER(provider_id_text) IN ('NULL', 'N/A', 'NA', 'TBD', 'UNKNOWN')
                                            OR provider_id_text IN ('-', NCHAR(8212))
                                        THEN NULL
                                        ELSE REPLACE(provider_id_text, ',', '')
                                    END,
                department_id_token = CASE
                                          WHEN department_id_text IS NULL OR department_id_text = ''
                                              OR UPPER(department_id_text) IN ('NULL', 'N/A', 'NA', 'TBD', 'UNKNOWN')
                                              OR department_id_text IN ('-', NCHAR(8212))
                                          THEN NULL
                                          ELSE REPLACE(department_id_text, ',', '')
                                      END,
                name_token = CASE
                                 WHEN name_text IS NULL OR name_text = ''
                                     OR UPPER(name_text) IN ('NULL', 'N/A', 'NA', 'TBD', 'UNKNOWN')
                                     OR name_text IN ('-', NCHAR(8212))
                                 THEN NULL
                                 ELSE REPLACE(REPLACE(name_text, '  ', ' '), '  ', ' ')
                             END,
                specialty_token = CASE
                                      WHEN specialty_text IS NULL OR specialty_text = ''
                                          OR UPPER(specialty_text) IN ('NULL', 'N/A', 'NA', 'TBD', 'UNKNOWN')
                                          OR specialty_text IN ('-', NCHAR(8212))
                                      THEN NULL
                                      ELSE REPLACE(REPLACE(specialty_text, '  ', ' '), '  ', ' ')
                                  END,
                years_experience_token = CASE
                                              WHEN years_experience_text IS NULL OR years_experience_text = ''
                                                  OR UPPER(years_experience_text) IN ('NULL', 'N/A', 'NA', 'TBD', 'UNKNOWN')
                                                  OR years_experience_text IN ('-', NCHAR(8212))
                                              THEN NULL
                                              ELSE REPLACE(years_experience_text, ',', '')
                                          END
            FROM source
        ),
        typed
        AS
        (
            SELECT
                provider_id = TRY_CONVERT(int, provider_id_token),
                department_id = TRY_CONVERT(int, department_id_token),
                name_collapsed = name_token,
                specialty_collapsed = specialty_token,
                years_experience_numeric = TRY_CONVERT(tinyint, years_experience_token)
            FROM tokens
        ),
        formatted
        AS
        (
            SELECT
                provider_id,
                department_id,
                name_formatted = CASE
                                     WHEN name_collapsed IS NULL THEN NULL
                                     ELSE (
                                         SELECT STRING_AGG(
                                                    CASE
                                                        WHEN word = 'dr' THEN 'Dr.'
                                                        WHEN LEN(word) <= 1 THEN UPPER(word)
                                                        ELSE CONCAT(UPPER(LEFT(word, 1)), LOWER(SUBSTRING(word, 2, LEN(word) - 1)))
                                                    END,
                                                    ' '
                                                ) WITHIN GROUP (ORDER BY ordinal)
                                         FROM (
                                                  SELECT LOWER(value) AS word, ordinal
                                                  FROM STRING_SPLIT(REPLACE(REPLACE(name_collapsed, ',', ' , '), '.', ' '), ' ', 1)
                                                  WHERE value <> ''
                                              ) AS words
                                     END,
                specialty_formatted = CASE
                                          WHEN specialty_collapsed IS NULL THEN NULL
                                          ELSE (
                                              SELECT STRING_AGG(
                                                         CASE
                                                             WHEN LEN(word) <= 1 THEN UPPER(word)
                                                             ELSE CONCAT(UPPER(LEFT(word, 1)), LOWER(SUBSTRING(word, 2, LEN(word) - 1)))
                                                         END,
                                                         ' '
                                                     ) WITHIN GROUP (ORDER BY ordinal)
                                              FROM (
                                                       SELECT LOWER(value) AS word, ordinal
                                                       FROM STRING_SPLIT(REPLACE(REPLACE(specialty_collapsed, ',', ' , '), '.', ' '), ' ', 1)
                                                       WHERE value <> ''
                                                   ) AS words
                                          )
                                      END,
                years_experience = CASE
                                       WHEN years_experience_numeric BETWEEN 0 AND 60 THEN years_experience_numeric
                                       ELSE NULL
                                   END
            FROM typed
        )
    SELECT
        provider_id,
        department_id,
        CASE WHEN name_formatted IS NULL THEN NULL ELSE CAST(name_formatted AS nvarchar(200)) END AS name,
        CASE WHEN specialty_formatted IS NULL THEN NULL ELSE CAST(specialty_formatted AS nvarchar(200)) END AS specialty,
        years_experience
    FROM formatted
    WHERE provider_id IS NOT NULL
        AND department_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE usage; all conversions via TRY_CONVERT.
-- Confirm: provider and department identifiers required for downstream joins.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.providers_std ORDER BY provider_id;
