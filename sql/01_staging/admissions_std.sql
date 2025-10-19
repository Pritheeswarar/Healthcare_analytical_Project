USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.admissions_std
AS
    /*
    Profiling-aligned staging view over dbo.admissions with inline standardization only.
    Placeholders collapse to NULL, then TRY_CONVERT / TRY_PARSE enforce resilient typing and categorical mapping.
*/
    SELECT
        typed.admission_id,
        typed.admission_date,
        typed.patient_id,
        typed.hospital_id,
        typed.department_id,
        typed.discharge_date,
        CASE
            WHEN typed.admission_type IS NULL OR LTRIM(RTRIM(typed.admission_type)) = '' THEN 'NA'
            ELSE typed.admission_type
        END AS admission_type,
        CASE
            WHEN typed.discharge_status IS NULL OR LTRIM(RTRIM(typed.discharge_status)) = '' THEN 'NA'
            ELSE typed.discharge_status
        END AS discharge_status,
        typed.length_of_stay,
        CASE
            WHEN typed.room_number IS NULL OR LTRIM(RTRIM(typed.room_number)) = '' THEN 'NA'
            ELSE typed.room_number
        END AS room_number,
        CASE
            WHEN typed.attending_physician IS NULL OR LTRIM(RTRIM(typed.attending_physician)) = '' THEN 'NA'
            ELSE typed.attending_physician
        END AS attending_physician,
        imputed.admission_date_imputed,
        imputed.discharge_date_imputed,
        date_outputs.is_admission_date_imputed,
        date_outputs.is_discharge_date_imputed,
        date_outputs.admission_date_final,
        date_outputs.discharge_date_final
    FROM dbo.admissions AS a
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.admission_id))), '') AS admission_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.admission_date))), '') AS admission_date_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.patient_id))), '') AS patient_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.hospital_id))), '') AS hospital_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.department_id))), '') AS department_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.discharge_date))), '') AS discharge_date_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.admission_type))), '') AS admission_type_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.discharge_status))), '') AS discharge_status_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.length_of_stay))), '') AS length_of_stay_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.room_number))), '') AS room_number_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), a.attending_physician))), '') AS attending_physician_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE WHEN trimmed.admission_id_raw IS NULL OR UPPER(trimmed.admission_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.admission_id_raw, ',', '') END AS admission_id_token,
            CASE WHEN trimmed.admission_date_raw IS NULL OR UPPER(trimmed.admission_date_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.admission_date_raw, '/', '-'), '.', '-') END AS admission_date_token,
            CASE WHEN trimmed.patient_id_raw IS NULL OR UPPER(trimmed.patient_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.patient_id_raw, ',', '') END AS patient_id_token,
            CASE WHEN trimmed.hospital_id_raw IS NULL OR UPPER(trimmed.hospital_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.hospital_id_raw, ',', '') END AS hospital_id_token,
            CASE WHEN trimmed.department_id_raw IS NULL OR UPPER(trimmed.department_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.department_id_raw, ',', '') END AS department_id_token,
            CASE WHEN trimmed.discharge_date_raw IS NULL OR UPPER(trimmed.discharge_date_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.discharge_date_raw, '/', '-'), '.', '-') END AS discharge_date_token,
            CASE WHEN trimmed.admission_type_raw IS NULL OR UPPER(trimmed.admission_type_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.admission_type_raw END AS admission_type_token,
            CASE WHEN trimmed.discharge_status_raw IS NULL OR UPPER(trimmed.discharge_status_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.discharge_status_raw END AS discharge_status_token,
            CASE WHEN trimmed.length_of_stay_raw IS NULL OR UPPER(trimmed.length_of_stay_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.length_of_stay_raw, ',', '') END AS length_of_stay_token,
            CASE WHEN trimmed.room_number_raw IS NULL OR UPPER(trimmed.room_number_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE UPPER(LEFT(trimmed.room_number_raw, 12)) END AS room_number_token,
            CASE WHEN trimmed.attending_physician_raw IS NULL OR UPPER(trimmed.attending_physician_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(TRIM(REPLACE(trimmed.attending_physician_raw, '  ', ' ')), ' ,', ',') END AS attending_physician_token
) AS tokens
CROSS APPLY (
    SELECT
            CASE
          WHEN tokens.attending_physician_token IS NULL THEN NULL
          ELSE NULLIF(
             LTRIM(
                CASE
                    WHEN UPPER(tokens.attending_physician_token) LIKE 'DR.%' THEN SUBSTRING(tokens.attending_physician_token, 4, LEN(tokens.attending_physician_token))
                    WHEN UPPER(tokens.attending_physician_token) LIKE 'DR %' THEN SUBSTRING(tokens.attending_physician_token, 3, LEN(tokens.attending_physician_token))
                    WHEN UPPER(tokens.attending_physician_token) LIKE 'DR%' THEN SUBSTRING(tokens.attending_physician_token, 3, LEN(tokens.attending_physician_token))
                    ELSE tokens.attending_physician_token
                END
             ),
             ''
          )
       END AS attending_physician_core
) AS physician_name
CROSS APPLY (
    SELECT
            TRY_CONVERT(int, tokens.admission_id_token) AS admission_id,
            COALESCE(
            TRY_CONVERT(date, tokens.admission_date_token, 23),
            TRY_CONVERT(date, tokens.admission_date_token, 120),
            TRY_CONVERT(date, tokens.admission_date_token, 121),
            TRY_CONVERT(date, tokens.admission_date_token, 111),
            TRY_CONVERT(date, tokens.admission_date_token, 112),
            TRY_CONVERT(date, tokens.admission_date_token, 101),
            TRY_CONVERT(date, tokens.admission_date_token, 103),
            TRY_CONVERT(date, tokens.admission_date_token, 105),
            TRY_CONVERT(date, tokens.admission_date_token, 107),
            TRY_PARSE(tokens.admission_date_token AS date USING 'en-US'),
            TRY_PARSE(tokens.admission_date_token AS date USING 'en-GB')
        ) AS admission_date,
            TRY_CONVERT(int, tokens.patient_id_token) AS patient_id,
            TRY_CONVERT(int, tokens.hospital_id_token) AS hospital_id,
            TRY_CONVERT(int, tokens.department_id_token) AS department_id,
            COALESCE(
            TRY_CONVERT(date, tokens.discharge_date_token, 23),
            TRY_CONVERT(date, tokens.discharge_date_token, 120),
            TRY_CONVERT(date, tokens.discharge_date_token, 121),
            TRY_CONVERT(date, tokens.discharge_date_token, 111),
            TRY_CONVERT(date, tokens.discharge_date_token, 112),
            TRY_CONVERT(date, tokens.discharge_date_token, 101),
            TRY_CONVERT(date, tokens.discharge_date_token, 103),
            TRY_CONVERT(date, tokens.discharge_date_token, 105),
            TRY_CONVERT(date, tokens.discharge_date_token, 107),
            TRY_PARSE(tokens.discharge_date_token AS date USING 'en-US'),
            TRY_PARSE(tokens.discharge_date_token AS date USING 'en-GB')
        ) AS discharge_date,
            CASE
            WHEN tokens.admission_type_token IS NULL THEN NULL
            ELSE CASE UPPER(tokens.admission_type_token)
                WHEN 'EMERGENCY' THEN N'Emergency'
                WHEN 'ER' THEN N'Emergency'
                WHEN 'E.R' THEN N'Emergency'
                WHEN 'TRAUMA' THEN N'Emergency'
                WHEN 'URGENT' THEN N'Urgent'
                WHEN 'ELECTIVE' THEN N'Elective'
                WHEN 'PLANNED' THEN N'Elective'
                WHEN 'SCHEDULED' THEN N'Elective'
                WHEN 'ROUTINE' THEN N'Elective'
                WHEN 'OBSERVATION' THEN N'Observation'
                WHEN 'OBS' THEN N'Observation'
                WHEN 'DAY CARE' THEN N'Observation'
                WHEN 'DAYCARE' THEN N'Observation'
                WHEN 'MATERNITY' THEN N'Maternity'
                WHEN 'MATERNAL' THEN N'Maternity'
                WHEN 'DELIVERY' THEN N'Maternity'
                WHEN 'NEWBORN' THEN N'Newborn'
                WHEN 'NB' THEN N'Newborn'
                WHEN 'OUTPATIENT' THEN N'Outpatient'
                WHEN 'OP' THEN N'Outpatient'
                ELSE CONCAT(UPPER(LEFT(tokens.admission_type_token, 1)), LOWER(SUBSTRING(tokens.admission_type_token, 2, LEN(tokens.admission_type_token))))
            END
        END AS admission_type,
            CASE
            WHEN tokens.discharge_status_token IS NULL THEN NULL
            ELSE CASE UPPER(tokens.discharge_status_token)
                WHEN 'DISCHARGED' THEN N'Discharged'
                WHEN 'HOME' THEN N'Discharged'
                WHEN 'STANDARD DISCHARGE' THEN N'Discharged'
                WHEN 'TRANSFERRED' THEN N'Transferred'
                WHEN 'TRANSFER' THEN N'Transferred'
                WHEN 'REFERRED' THEN N'Transferred'
                WHEN 'MOVED' THEN N'Transferred'
                WHEN 'AMA' THEN N'Left Against Medical Advice'
                WHEN 'LAMA' THEN N'Left Against Medical Advice'
                WHEN 'LEFT AGAINST MEDICAL ADVICE' THEN N'Left Against Medical Advice'
                WHEN 'LEFT_AGAINST_MEDICAL_ADVICE' THEN N'Left Against Medical Advice'
                WHEN 'EXPIRED' THEN N'Expired'
                WHEN 'DECEASED' THEN N'Expired'
                WHEN 'DEAD' THEN N'Expired'
                WHEN 'PENDING' THEN N'Pending'
                WHEN 'IN PROGRESS' THEN N'Pending'
                WHEN 'ONGOING' THEN N'Pending'
                WHEN 'HOSPICE' THEN N'Hospice'
                WHEN 'SNF' THEN N'Skilled Nursing Facility'
                WHEN 'SKILLED NURSING' THEN N'Skilled Nursing Facility'
                WHEN 'REHAB' THEN N'Rehabilitation'
                WHEN 'REHABILITATION' THEN N'Rehabilitation'
                ELSE CONCAT(UPPER(LEFT(tokens.discharge_status_token, 1)), LOWER(SUBSTRING(tokens.discharge_status_token, 2, LEN(tokens.discharge_status_token))))
            END
        END AS discharge_status,
            TRY_CONVERT(int, tokens.length_of_stay_token) AS length_of_stay,
            tokens.room_number_token AS room_number,
            CASE
                WHEN physician_name.attending_physician_core IS NULL THEN NULL
                ELSE CONCAT(
                    'Dr. ',
                    CONCAT(
                        UPPER(LEFT(physician_name.attending_physician_core, 1)),
                        LOWER(SUBSTRING(physician_name.attending_physician_core, 2, LEN(physician_name.attending_physician_core)))
                    )
                )
            END AS attending_physician
) AS typed
CROSS APPLY (
    SELECT
            CASE
            WHEN typed.discharge_date IS NOT NULL AND typed.length_of_stay IS NOT NULL
                THEN TRY_CONVERT(date, DATEADD(day, -typed.length_of_stay, typed.discharge_date))
            ELSE NULL
        END AS admission_date_imputed,
            CASE
            WHEN typed.admission_date IS NOT NULL AND typed.length_of_stay IS NOT NULL
                THEN TRY_CONVERT(date, DATEADD(day, typed.length_of_stay, typed.admission_date))
            ELSE NULL
        END AS discharge_date_imputed
) AS imputed
CROSS APPLY (
    SELECT
            CASE
            WHEN typed.admission_date IS NULL AND imputed.admission_date_imputed IS NOT NULL THEN CAST(1 AS bit)
            ELSE CAST(0 AS bit)
        END AS is_admission_date_imputed,
            CASE
            WHEN typed.discharge_date IS NULL AND imputed.discharge_date_imputed IS NOT NULL THEN CAST(1 AS bit)
            ELSE CAST(0 AS bit)
        END AS is_discharge_date_imputed,
            COALESCE(typed.admission_date, imputed.admission_date_imputed) AS admission_date_final,
            COALESCE(typed.discharge_date, imputed.discharge_date_imputed) AS discharge_date_final
) AS date_outputs;
GO

/*
-- Verification snippet:
-- SELECT TOP (25) *
-- FROM stg.admissions_std
-- ORDER BY admission_date, admission_id;
*/
