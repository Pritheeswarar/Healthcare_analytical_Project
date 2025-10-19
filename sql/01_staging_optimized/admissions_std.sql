/*
    Source table: dbo.admissions
    Transformed columns: admission_id, admission_date, patient_id, hospital_id, department_id, discharge_date,
        admission_type, discharge_status, length_of_stay, room_number, attending_physician,
        admission_date_imputed, discharge_date_imputed, is_admission_date_imputed,
        is_discharge_date_imputed, admission_date_final, discharge_date_final
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'TBD', '-'
    Date styles used: 23, 120, 121, 111, 112, 101, 103, 105, 107
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.admissions_std
AS
    WITH
        source
        AS
        (
            SELECT
                admission_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.admission_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                admission_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.admission_date), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                patient_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.patient_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                hospital_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.hospital_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                department_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.department_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                discharge_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.discharge_date), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                admission_type_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.admission_type), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                discharge_status_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.discharge_status), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                length_of_stay_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.length_of_stay), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                room_number_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.room_number), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                attending_physician_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), a.attending_physician), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.admissions AS a
        ),
        tokens
        AS
        (
            SELECT
                admission_id_token = CASE
            WHEN admission_id_text IS NULL OR admission_id_text = ''
                    OR UPPER(admission_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(admission_id_text, ',', '')
        END,
                admission_date_token = CASE
            WHEN admission_date_text IS NULL OR admission_date_text = ''
                    OR UPPER(admission_date_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(admission_date_text, '/', '-'), '.', '-')
        END,
                patient_id_token = CASE
            WHEN patient_id_text IS NULL OR patient_id_text = ''
                    OR UPPER(patient_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(patient_id_text, ',', '')
        END,
                hospital_id_token = CASE
            WHEN hospital_id_text IS NULL OR hospital_id_text = ''
                    OR UPPER(hospital_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(hospital_id_text, ',', '')
        END,
                department_id_token = CASE
            WHEN department_id_text IS NULL OR department_id_text = ''
                    OR UPPER(department_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(department_id_text, ',', '')
        END,
                discharge_date_token = CASE
            WHEN discharge_date_text IS NULL OR discharge_date_text = ''
                    OR UPPER(discharge_date_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(discharge_date_text, '/', '-'), '.', '-')
        END,
                admission_type_token = CASE
            WHEN admission_type_text IS NULL OR admission_type_text = ''
                    OR UPPER(admission_type_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE admission_type_text
        END,
                discharge_status_token = CASE
            WHEN discharge_status_text IS NULL OR discharge_status_text = ''
                    OR UPPER(discharge_status_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE discharge_status_text
        END,
                length_of_stay_token = CASE
            WHEN length_of_stay_text IS NULL OR length_of_stay_text = ''
                    OR UPPER(length_of_stay_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(length_of_stay_text, ',', '')
        END,
                room_number_token = CASE
            WHEN room_number_text IS NULL OR room_number_text = ''
                    OR UPPER(room_number_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE UPPER(LEFT(room_number_text, 12))
        END,
                attending_physician_token = CASE
            WHEN attending_physician_text IS NULL OR attending_physician_text = ''
                    OR UPPER(attending_physician_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(TRIM(REPLACE(attending_physician_text, '  ', ' ')), ' ,', ',')
        END,
                attending_physician_core = CASE
            WHEN attending_physician_text IS NULL OR attending_physician_text = ''
                    OR UPPER(attending_physician_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE NULLIF(LTRIM(
                CASE
                    WHEN UPPER(attending_physician_text) LIKE 'DR.%' THEN SUBSTRING(attending_physician_text, 4, 4000)
                    WHEN UPPER(attending_physician_text) LIKE 'DR %' THEN SUBSTRING(attending_physician_text, 3, 4000)
                    WHEN UPPER(attending_physician_text) LIKE 'DR%' THEN SUBSTRING(attending_physician_text, 3, 4000)
                    ELSE attending_physician_text
                END
            ), '')
        END
            FROM source
        ),
        typed
        AS
        (
            SELECT
                tokens.*,
                admission_id = TRY_CONVERT(int, admission_id_token),
                admission_date = COALESCE(
            TRY_CONVERT(date, admission_date_token, 23),
            TRY_CONVERT(date, admission_date_token, 120),
            TRY_CONVERT(date, admission_date_token, 121),
            TRY_CONVERT(date, admission_date_token, 111),
            TRY_CONVERT(date, admission_date_token, 112),
            TRY_CONVERT(date, admission_date_token, 101),
            TRY_CONVERT(date, admission_date_token, 103),
            TRY_CONVERT(date, admission_date_token, 105),
            TRY_CONVERT(date, admission_date_token, 107)
        ),
                patient_id = TRY_CONVERT(int, patient_id_token),
                hospital_id = TRY_CONVERT(int, hospital_id_token),
                department_id = TRY_CONVERT(int, department_id_token),
                discharge_date = COALESCE(
            TRY_CONVERT(date, discharge_date_token, 23),
            TRY_CONVERT(date, discharge_date_token, 120),
            TRY_CONVERT(date, discharge_date_token, 121),
            TRY_CONVERT(date, discharge_date_token, 111),
            TRY_CONVERT(date, discharge_date_token, 112),
            TRY_CONVERT(date, discharge_date_token, 101),
            TRY_CONVERT(date, discharge_date_token, 103),
            TRY_CONVERT(date, discharge_date_token, 105),
            TRY_CONVERT(date, discharge_date_token, 107)
        ),
                admission_type_value = CASE
            WHEN admission_type_token IS NULL THEN NULL
            ELSE CASE UPPER(admission_type_token)
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
                ELSE CONCAT(UPPER(LEFT(admission_type_token, 1)), LOWER(SUBSTRING(admission_type_token, 2, LEN(admission_type_token))))
            END
        END,
                discharge_status_value = CASE
            WHEN discharge_status_token IS NULL THEN NULL
            ELSE CASE UPPER(discharge_status_token)
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
                ELSE CONCAT(UPPER(LEFT(discharge_status_token, 1)), LOWER(SUBSTRING(discharge_status_token, 2, LEN(discharge_status_token))))
            END
        END,
                length_of_stay_value = TRY_CONVERT(int, length_of_stay_token),
                attending_physician_value = CASE
            WHEN attending_physician_core IS NULL THEN NULL
            ELSE CONCAT('Dr. ',
                    CONCAT(
                        UPPER(LEFT(attending_physician_core, 1)),
                        LOWER(SUBSTRING(attending_physician_core, 2, LEN(attending_physician_core)))
                    )
                )
        END
            FROM tokens
        )
    SELECT
        admission_id,
        admission_date,
        patient_id,
        hospital_id,
        department_id,
        discharge_date,
        admission_type = CASE
        WHEN admission_type_value IS NULL OR LTRIM(RTRIM(admission_type_value)) = '' THEN 'NA'
        ELSE admission_type_value
    END,
        discharge_status = CASE
        WHEN discharge_status_value IS NULL OR LTRIM(RTRIM(discharge_status_value)) = '' THEN 'NA'
        ELSE discharge_status_value
    END,
        length_of_stay = length_of_stay_value,
        room_number = CASE
        WHEN room_number_token IS NULL OR LTRIM(RTRIM(room_number_token)) = '' THEN 'NA'
        ELSE room_number_token
    END,
        attending_physician = CASE
        WHEN attending_physician_value IS NULL OR LTRIM(RTRIM(attending_physician_value)) = '' THEN 'NA'
        ELSE attending_physician_value
    END,
        admission_date_imputed = CASE
        WHEN discharge_date IS NOT NULL AND length_of_stay_value IS NOT NULL
            THEN TRY_CONVERT(date, DATEADD(day, -length_of_stay_value, discharge_date))
        ELSE NULL
    END,
        discharge_date_imputed = CASE
        WHEN admission_date IS NOT NULL AND length_of_stay_value IS NOT NULL
            THEN TRY_CONVERT(date, DATEADD(day, length_of_stay_value, admission_date))
        ELSE NULL
    END,
        is_admission_date_imputed = CASE
        WHEN admission_date IS NULL AND discharge_date IS NOT NULL AND length_of_stay_value IS NOT NULL THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
    END,
        is_discharge_date_imputed = CASE
        WHEN discharge_date IS NULL AND admission_date IS NOT NULL AND length_of_stay_value IS NOT NULL THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
    END,
        admission_date_final = COALESCE(admission_date, CASE
        WHEN discharge_date IS NOT NULL AND length_of_stay_value IS NOT NULL
            THEN TRY_CONVERT(date, DATEADD(day, -length_of_stay_value, discharge_date))
        ELSE NULL
    END),
        discharge_date_final = COALESCE(discharge_date, CASE
        WHEN admission_date IS NOT NULL AND length_of_stay_value IS NOT NULL
            THEN TRY_CONVERT(date, DATEADD(day, length_of_stay_value, admission_date))
        ELSE NULL
    END)
    FROM typed
    WHERE admission_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no FORMAT, no CROSS APPLY.
-- Confirm: no scalar UDFs referenced.
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.admissions_std ORDER BY admission_date, admission_id;
