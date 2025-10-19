/*
    Source table: dbo.patients
    Transformed columns: patient_id, mrn, gender, blood_group, insurance_type, area, pincode, date_of_birth, created_date
    Placeholder tokens nullified: '', 'NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE', '-', NCHAR(8212)
    Date styles used: 23 (yyyy-mm-dd), 101 (mm/dd/yyyy), 103 (dd/mm/yyyy), 112 (yyyymmdd), 120/121 (ISO datetime)
*/
USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg_optimized')
    EXEC('CREATE SCHEMA stg_optimized');
GO

CREATE OR ALTER VIEW stg_optimized.patients_std
AS
    WITH
        source
        AS
        (
            SELECT
                patient_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(50), src.patient_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                mrn_text = TRIM(TRANSLATE(CONVERT(nvarchar(50), src.mrn), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                gender_text = TRIM(TRANSLATE(CONVERT(nvarchar(30), src.gender), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                blood_group_text = TRIM(TRANSLATE(CONVERT(nvarchar(10), src.blood_group), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                insurance_text = TRIM(TRANSLATE(CONVERT(nvarchar(60), src.insurance_type), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                area_text = TRIM(TRANSLATE(CONVERT(nvarchar(120), src.area), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                pincode_text = TRIM(TRANSLATE(CONVERT(nvarchar(20), src.pincode), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                dob_text = TRIM(TRANSLATE(CONVERT(nvarchar(60), src.date_of_birth), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                created_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(60), src.created_date), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.patients AS src
        ),
        tokens
        AS
        (
            SELECT
                patient_id_token = CASE
            WHEN patient_id_text IS NULL OR patient_id_text = ''
                    OR UPPER(patient_id_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR patient_id_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(patient_id_text, ',', '')
        END,
                mrn_token = CASE
            WHEN mrn_text IS NULL OR mrn_text = ''
                    OR UPPER(mrn_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR mrn_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE mrn_text
        END,
                gender_token = CASE
            WHEN gender_text IS NULL OR gender_text = ''
                    OR UPPER(gender_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR gender_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE UPPER(gender_text)
        END,
                blood_group_token = CASE
            WHEN blood_group_text IS NULL OR blood_group_text = ''
                    OR UPPER(blood_group_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR blood_group_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE UPPER(REPLACE(blood_group_text, ' ', ''))
        END,
                insurance_token = CASE
            WHEN insurance_text IS NULL OR insurance_text = ''
                    OR UPPER(insurance_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR insurance_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE UPPER(REPLACE(insurance_text, '-', ' '))
        END,
                area_token = CASE
            WHEN area_text IS NULL OR area_text = ''
                    OR UPPER(area_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR area_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(REPLACE(area_text, '  ', ' '), '  ', ' ')
        END,
                pincode_token = CASE
            WHEN pincode_text IS NULL OR pincode_text = ''
                    OR UPPER(pincode_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR pincode_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE REPLACE(pincode_text, ' ', '')
        END,
                dob_token = CASE
            WHEN dob_text IS NULL OR dob_text = ''
                    OR UPPER(dob_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR dob_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE dob_text
        END,
                created_date_token = CASE
            WHEN created_date_text IS NULL OR created_date_text = ''
                    OR UPPER(created_date_text) IN ('NULL', 'N/A', 'NA', 'NOT PROVIDED', 'PENDING', 'TBD', 'UNKNOWN', 'NONE')
                    OR created_date_text IN ('-', NCHAR(8212))
            THEN NULL
            ELSE created_date_text
        END
            FROM source
        )
    SELECT
        patient_id = TRY_CONVERT(int, patient_id_token),
        mrn = mrn_token,
        gender = CASE
        WHEN gender_token IN ('M', 'MALE') THEN 'Male'
        WHEN gender_token IN ('F', 'FEMALE') THEN 'Female'
        WHEN gender_token IN ('OTHER', 'O') THEN 'Other'
        ELSE NULL
    END,
        blood_group = CASE blood_group_token
        WHEN 'A+' THEN 'A+'
        WHEN 'A-' THEN 'A-'
        WHEN 'B+' THEN 'B+'
        WHEN 'B-' THEN 'B-'
        WHEN 'AB+' THEN 'AB+'
        WHEN 'AB-' THEN 'AB-'
        WHEN 'O+' THEN 'O+'
        WHEN 'O-' THEN 'O-'
        ELSE NULL
    END,
        insurance_type = CASE insurance_token
        WHEN 'GOVERNMENT' THEN 'Government'
        WHEN 'GOV' THEN 'Government'
        WHEN 'PRIVATE' THEN 'Private'
        WHEN 'PVT' THEN 'Private'
        WHEN 'SELF PAY' THEN 'Self Pay'
        WHEN 'SELF  PAY' THEN 'Self Pay'
        WHEN 'SELF' THEN 'Self Pay'
        WHEN 'CORPORATE' THEN 'Corporate'
        WHEN 'CORP' THEN 'Corporate'
        ELSE NULL
    END,
        area = CASE WHEN area_token IS NULL THEN NULL ELSE area_token END,
        pincode = CASE
        WHEN pincode_token LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]' THEN pincode_token
        WHEN TRY_CONVERT(int, pincode_token) BETWEEN 100000 AND 999999
            THEN RIGHT('000000' + CONVERT(varchar(10), TRY_CONVERT(int, pincode_token)), 6)
        ELSE NULL
    END,
        date_of_birth = COALESCE(
        TRY_CONVERT(date, dob_token, 23),
        TRY_CONVERT(date, dob_token, 112),
        TRY_CONVERT(date, dob_token, 101),
        TRY_CONVERT(date, dob_token, 103)
    ),
        created_date = COALESCE(
        TRY_CONVERT(datetime2(0), created_date_token, 121),
        TRY_CONVERT(datetime2(0), created_date_token, 120),
        TRY_CONVERT(datetime2(0), created_date_token, 126),
        TRY_CONVERT(datetime2(0), created_date_token, 23)
    )
    FROM tokens;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no FORMAT, no CROSS APPLY.
-- Confirm: no scalar UDFs in the SELECT list.
-- Confirm: only dbo sources referenced.
-- Confirm: column aliases align with legacy business rules.
-- Smoke test:
-- SELECT TOP (50) * FROM stg_optimized.patients_std WHERE patient_id IS NOT NULL;
