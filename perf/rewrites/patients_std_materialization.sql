USE [Healthcare];
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

/* Ensure we have a reusable numbers helper (1..400 covers current token logic) */
IF OBJECT_ID('stg.Numbers', 'U') IS NULL
BEGIN
    CREATE TABLE stg.Numbers
    (
        n int NOT NULL PRIMARY KEY
    );
END;

;
WITH
    seq
    AS
    (
        SELECT TOP (400)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
        FROM sys.all_objects AS ao
    )
INSERT INTO stg.Numbers
    (n)
SELECT s.n
FROM seq AS s
    LEFT JOIN stg.Numbers AS existing ON existing.n = s.n
WHERE existing.n IS NULL;

IF OBJECT_ID('stg.patients_std_tbl', 'U') IS NULL
BEGIN
    CREATE TABLE stg.patients_std_tbl
    (
        patient_id int NOT NULL,
        mrn nvarchar(30) NULL,
        first_name nvarchar(60) NULL,
        last_name nvarchar(60) NULL,
        date_of_birth date NULL,
        age tinyint NULL,
        gender nvarchar(10) NULL,
        blood_group nvarchar(3) NULL,
        phone nvarchar(16) NULL,
        address nvarchar(250) NULL,
        area nvarchar(60) NULL,
        pincode int NULL,
        insurance_type nvarchar(20) NULL,
        emergency_contact nvarchar(200) NULL,
        created_date datetime2(0) NULL,
        CONSTRAINT PK_patients_std_tbl PRIMARY KEY CLUSTERED (patient_id)
    );
END;

TRUNCATE TABLE stg.patients_std_tbl;

INSERT INTO stg.patients_std_tbl
    (
    patient_id,
    mrn,
    first_name,
    last_name,
    date_of_birth,
    age,
    gender,
    blood_group,
    phone,
    address,
    area,
    pincode,
    insurance_type,
    emergency_contact,
    created_date
    )
SELECT
    typed.patient_id,
    typed.mrn,
    typed.first_name,
    typed.last_name,
    typed.date_of_birth,
    typed.age,
    typed.gender,
    typed.blood_group,
    typed.phone,
    typed.address,
    typed.area,
    typed.pincode,
    typed.insurance_type,
    typed.emergency_contact,
    typed.created_date
FROM dbo.patients AS p
CROSS APPLY (
    SELECT
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(50), p.patient_id))), '') AS patient_id_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(50), p.mrn))), '') AS mrn_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(120), p.first_name))), '') AS first_name_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(120), p.last_name))), '') AS last_name_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(60), p.date_of_birth))), '') AS date_of_birth_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(10), p.age))), '') AS age_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(30), p.gender))), '') AS gender_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(10), p.blood_group))), '') AS blood_group_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), p.phone))), '') AS phone_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(400), p.address))), '') AS address_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(120), p.area))), '') AS area_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(20), p.pincode))), '') AS pincode_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(60), p.insurance_type))), '') AS insurance_type_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(400), p.emergency_contact))), '') AS emergency_contact_raw,
        NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(40), p.created_date))), '') AS created_date_raw
) AS trimmed
CROSS APPLY (
    SELECT
        CASE WHEN trimmed.patient_id_raw IS NULL OR UPPER(trimmed.patient_id_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.patient_id_raw IN ('-', NCHAR(8212)) THEN NULL ELSE REPLACE(trimmed.patient_id_raw, ',', '') END AS patient_id_token,
        CASE WHEN trimmed.mrn_raw IS NULL OR UPPER(trimmed.mrn_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.mrn_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.mrn_raw END AS mrn_token,
        CASE WHEN trimmed.first_name_raw IS NULL OR UPPER(trimmed.first_name_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.first_name_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.first_name_raw END AS first_name_token,
        CASE WHEN trimmed.last_name_raw IS NULL OR UPPER(trimmed.last_name_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.last_name_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.last_name_raw END AS last_name_token,
        CASE WHEN trimmed.date_of_birth_raw IS NULL OR UPPER(trimmed.date_of_birth_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.date_of_birth_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.date_of_birth_raw END AS date_of_birth_token,
        CASE WHEN trimmed.age_raw IS NULL OR UPPER(trimmed.age_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.age_raw IN ('-', NCHAR(8212)) THEN NULL ELSE REPLACE(trimmed.age_raw, ',', '') END AS age_token,
        CASE WHEN trimmed.gender_raw IS NULL OR UPPER(trimmed.gender_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.gender_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.gender_raw END AS gender_token,
        CASE WHEN trimmed.blood_group_raw IS NULL OR UPPER(trimmed.blood_group_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.blood_group_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.blood_group_raw END AS blood_group_token,
        CASE WHEN trimmed.phone_raw IS NULL OR UPPER(trimmed.phone_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.phone_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.phone_raw END AS phone_token,
        CASE WHEN trimmed.address_raw IS NULL OR UPPER(trimmed.address_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.address_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.address_raw END AS address_token,
        CASE WHEN trimmed.area_raw IS NULL OR UPPER(trimmed.area_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.area_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.area_raw END AS area_token,
        CASE WHEN trimmed.pincode_raw IS NULL OR UPPER(trimmed.pincode_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.pincode_raw IN ('-', NCHAR(8212)) THEN NULL ELSE REPLACE(trimmed.pincode_raw, ',', '') END AS pincode_token,
        CASE WHEN trimmed.insurance_type_raw IS NULL OR UPPER(trimmed.insurance_type_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.insurance_type_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.insurance_type_raw END AS insurance_type_token,
        CASE WHEN trimmed.emergency_contact_raw IS NULL OR UPPER(trimmed.emergency_contact_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.emergency_contact_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.emergency_contact_raw END AS emergency_contact_token,
        CASE WHEN trimmed.created_date_raw IS NULL OR UPPER(trimmed.created_date_raw) IN ('NULL', 'N/A', 'NOT PROVIDED', 'PENDING', 'TBD') OR trimmed.created_date_raw IN ('-', NCHAR(8212)) THEN NULL ELSE trimmed.created_date_raw END AS created_date_token
) AS tokens
CROSS APPLY (SELECT TRY_CONVERT(int, tokens.patient_id_token) AS patient_id) AS patient_id_val
CROSS APPLY (
    SELECT
        CASE WHEN tokens.first_name_token IS NULL THEN NULL ELSE REPLACE(REPLACE(REPLACE(tokens.first_name_token, '  ', ' '), '  ', ' '), '  ', ' ') END AS first_name_collapsed,
        CASE WHEN tokens.last_name_token IS NULL THEN NULL ELSE REPLACE(REPLACE(REPLACE(tokens.last_name_token, '  ', ' '), '  ', ' '), '  ', ' ') END AS last_name_collapsed
) AS name_prep
CROSS APPLY (
    SELECT
        CASE WHEN name_prep.first_name_collapsed IS NULL THEN NULL ELSE LOWER(name_prep.first_name_collapsed) END AS first_name_lower,
        CASE WHEN name_prep.first_name_collapsed IS NULL THEN 0 ELSE LEN(name_prep.first_name_collapsed) END AS first_name_len,
        CASE WHEN name_prep.last_name_collapsed IS NULL THEN NULL ELSE LOWER(name_prep.last_name_collapsed) END AS last_name_lower,
        CASE WHEN name_prep.last_name_collapsed IS NULL THEN 0 ELSE LEN(name_prep.last_name_collapsed) END AS last_name_len
) AS name_lower
OUTER APPLY (
    SELECT CASE
        WHEN name_lower.first_name_len <= 0 THEN NULL
        ELSE (
            SELECT STRING_AGG(
                       CASE WHEN chars.is_word_start = 1 THEN chars.upper_char ELSE chars.curr_char END,
                       ''
                   ) WITHIN GROUP (ORDER BY chars.pos)
        FROM (
                SELECT
                num.n AS pos,
                SUBSTRING(name_lower.first_name_lower, num.n, 1) AS curr_char,
                UPPER(SUBSTRING(name_lower.first_name_lower, num.n, 1)) AS upper_char,
                CASE WHEN num.n = 1 THEN 1 WHEN SUBSTRING(name_lower.first_name_lower, num.n - 1, 1) IN (' ', '-', NCHAR(39)) THEN 1 ELSE 0 END AS is_word_start
            FROM stg.Numbers AS num
            WHERE num.n <= name_lower.first_name_len
            ) AS chars
        )
    END AS first_name_title
) AS first_title
OUTER APPLY (
    SELECT CASE
        WHEN name_lower.last_name_len <= 0 THEN NULL
        ELSE (
            SELECT STRING_AGG(
                       CASE WHEN chars.is_word_start = 1 THEN chars.upper_char ELSE chars.curr_char END,
                       ''
                   ) WITHIN GROUP (ORDER BY chars.pos)
        FROM (
                SELECT
                num.n AS pos,
                SUBSTRING(name_lower.last_name_lower, num.n, 1) AS curr_char,
                UPPER(SUBSTRING(name_lower.last_name_lower, num.n, 1)) AS upper_char,
                CASE WHEN num.n = 1 THEN 1 WHEN SUBSTRING(name_lower.last_name_lower, num.n - 1, 1) IN (' ', '-', NCHAR(39)) THEN 1 ELSE 0 END AS is_word_start
            FROM stg.Numbers AS num
            WHERE num.n <= name_lower.last_name_len
            ) AS chars
        )
    END AS last_name_title
) AS last_title
OUTER APPLY (
    SELECT CASE WHEN tokens.mrn_token IS NULL THEN NULL WHEN UPPER(LEFT(tokens.mrn_token, 3)) = 'MRN' THEN CONCAT('MRN', SUBSTRING(tokens.mrn_token, 4, 27)) ELSE tokens.mrn_token END AS mrn_standardized
) AS mrn_norm
OUTER APPLY (
    SELECT COALESCE(
        TRY_CONVERT(date, tokens.date_of_birth_token, 23),
        TRY_CONVERT(date, tokens.date_of_birth_token, 126),
        TRY_CONVERT(date, tokens.date_of_birth_token, 112),
        TRY_CONVERT(date, tokens.date_of_birth_token, 111),
        CASE WHEN tokens.date_of_birth_token LIKE '%[A-Za-z]%' THEN TRY_PARSE(tokens.date_of_birth_token AS date USING 'en-US') END,
        CASE WHEN tokens.date_of_birth_token LIKE '%[A-Za-z]%' THEN TRY_PARSE(tokens.date_of_birth_token AS date USING 'en-GB') END
    ) AS date_of_birth
) AS dob_norm
CROSS APPLY (SELECT TRY_CONVERT(tinyint, tokens.age_token) AS age_value_raw) AS age_raw_val
CROSS APPLY (
    SELECT CASE
        WHEN age_raw_val.age_value_raw IS NULL THEN NULL
        WHEN age_raw_val.age_value_raw BETWEEN 0 AND 120 THEN age_raw_val.age_value_raw
        ELSE NULL
    END AS age_value
) AS age_norm
OUTER APPLY (
    SELECT CASE
        WHEN tokens.gender_token IS NULL THEN NULL
        WHEN UPPER(tokens.gender_token) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(tokens.gender_token) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(tokens.gender_token) IN ('OTHER', 'O') THEN 'Other'
        ELSE NULL
    END AS gender
) AS gender_norm
OUTER APPLY (
    SELECT CASE
        WHEN tokens.blood_group_token IS NULL THEN NULL
        ELSE CASE UPPER(REPLACE(tokens.blood_group_token, ' ', ''))
            WHEN 'A+' THEN 'A+'
            WHEN 'A-' THEN 'A-'
            WHEN 'B+' THEN 'B+'
            WHEN 'B-' THEN 'B-'
            WHEN 'AB+' THEN 'AB+'
            WHEN 'AB-' THEN 'AB-'
            WHEN 'O+' THEN 'O+'
            WHEN 'O-' THEN 'O-'
            ELSE NULL
        END
    END AS blood_group
) AS blood_norm
OUTER APPLY (
    SELECT
        CASE WHEN tokens.phone_token IS NULL THEN NULL ELSE LOWER(tokens.phone_token) END AS phone_lower,
        CASE WHEN tokens.phone_token IS NULL THEN NULL ELSE REPLACE(REPLACE(REPLACE(REPLACE(tokens.phone_token, ' ', ''), '-', ''), '(', ''), ')', '') END AS phone_compact
) AS phone_prep
OUTER APPLY (
    SELECT CASE WHEN phone_prep.phone_compact IS NULL THEN NULL ELSE REPLACE(phone_prep.phone_compact, '+', '') END AS phone_digits_only
) AS phone_digits
OUTER APPLY (
    SELECT CASE
        WHEN phone_prep.phone_compact IS NULL THEN NULL
        WHEN phone_prep.phone_lower LIKE '%e+%' OR phone_prep.phone_lower LIKE '%e-%' THEN NULL
        WHEN PATINDEX('%[^0-9+]%', phone_prep.phone_compact) > 0 THEN NULL
        WHEN phone_digits.phone_digits_only IS NULL OR phone_digits.phone_digits_only = '' THEN NULL
        WHEN LEN(phone_digits.phone_digits_only) = 10 THEN CONCAT('+91', phone_digits.phone_digits_only)
        WHEN LEN(phone_digits.phone_digits_only) = 11 AND LEFT(phone_digits.phone_digits_only, 1) = '0' THEN CONCAT('+91', RIGHT(phone_digits.phone_digits_only, 10))
        WHEN LEN(phone_digits.phone_digits_only) = 12 AND LEFT(phone_digits.phone_digits_only, 2) = '91' THEN CONCAT('+', phone_digits.phone_digits_only)
        WHEN LEFT(phone_prep.phone_compact, 3) = '+91' AND LEN(phone_digits.phone_digits_only) = 12 THEN phone_prep.phone_compact
        ELSE NULL
    END AS phone_normalized
) AS phone_norm
OUTER APPLY (
    SELECT CASE WHEN tokens.address_token IS NULL THEN NULL ELSE REPLACE(REPLACE(REPLACE(tokens.address_token, '  ', ' '), '  ', ' '), '  ', ' ') END AS address_collapsed
) AS address_clean
OUTER APPLY (
    SELECT CASE WHEN tokens.area_token IS NULL THEN NULL ELSE REPLACE(REPLACE(REPLACE(tokens.area_token, '  ', ' '), '  ', ' '), '  ', ' ') END AS area_collapsed
) AS area_prep
CROSS APPLY (
    SELECT CASE WHEN area_prep.area_collapsed IS NULL THEN NULL ELSE LOWER(area_prep.area_collapsed) END AS area_lower,
        CASE WHEN area_prep.area_collapsed IS NULL THEN 0 ELSE LEN(area_prep.area_collapsed) END AS area_len
) AS area_lower_info
OUTER APPLY (
    SELECT CASE
        WHEN area_lower_info.area_len <= 0 THEN NULL
        ELSE (
            SELECT STRING_AGG(
                       CASE WHEN chars.is_word_start = 1 THEN chars.upper_char ELSE chars.curr_char END,
                       ''
                   ) WITHIN GROUP (ORDER BY chars.pos)
        FROM (
                SELECT
                num.n AS pos,
                SUBSTRING(area_lower_info.area_lower, num.n, 1) AS curr_char,
                UPPER(SUBSTRING(area_lower_info.area_lower, num.n, 1)) AS upper_char,
                CASE WHEN num.n = 1 THEN 1 WHEN SUBSTRING(area_lower_info.area_lower, num.n - 1, 1) IN (' ', '-', NCHAR(39)) THEN 1 ELSE 0 END AS is_word_start
            FROM stg.Numbers AS num
            WHERE num.n <= area_lower_info.area_len
            ) AS chars
        )
    END AS area_title_case
) AS area_title
CROSS APPLY (SELECT TRY_CONVERT(int, tokens.pincode_token) AS pincode_raw) AS pincode_try
CROSS APPLY (
    SELECT CASE
        WHEN pincode_try.pincode_raw BETWEEN 100000 AND 999999 THEN pincode_try.pincode_raw
        ELSE NULL
    END AS pincode
) AS pincode_norm
OUTER APPLY (
    SELECT CASE
        WHEN tokens.insurance_type_token IS NULL THEN NULL
        WHEN UPPER(tokens.insurance_type_token) IN ('GOVERNMENT', 'GOV') THEN 'Government'
        WHEN UPPER(tokens.insurance_type_token) IN ('PRIVATE', 'PVT') THEN 'Private'
        WHEN UPPER(tokens.insurance_type_token) IN ('SELF', 'SELF PAY') THEN 'Self Pay'
        WHEN UPPER(tokens.insurance_type_token) IN ('CORPORATE', 'CORP') THEN 'Corporate'
        ELSE NULL
    END AS insurance_type
) AS insurance_norm
OUTER APPLY (
    SELECT CASE WHEN tokens.emergency_contact_token IS NULL THEN NULL ELSE LTRIM(RTRIM(tokens.emergency_contact_token)) END AS contact_trimmed
) AS contact_base
OUTER APPLY (
    SELECT CASE
        WHEN contact_base.contact_trimmed IS NULL THEN NULL
        WHEN UPPER(contact_base.contact_trimmed) LIKE 'CONTACT:%' AND CHARINDEX(':', contact_base.contact_trimmed) > 0 THEN LTRIM(RTRIM(SUBSTRING(contact_base.contact_trimmed, CHARINDEX(':', contact_base.contact_trimmed) + 1, 4000)))
        WHEN UPPER(contact_base.contact_trimmed) LIKE 'CONTACT-%' AND CHARINDEX('-', contact_base.contact_trimmed) > 0 THEN LTRIM(RTRIM(SUBSTRING(contact_base.contact_trimmed, CHARINDEX('-', contact_base.contact_trimmed) + 1, 4000)))
        WHEN UPPER(contact_base.contact_trimmed) LIKE 'CONTACT %' THEN LTRIM(RTRIM(SUBSTRING(contact_base.contact_trimmed, LEN('Contact ') + 1, 4000)))
        ELSE contact_base.contact_trimmed
    END AS contact_without_prefix
) AS contact_prefix
OUTER APPLY (
    SELECT CASE WHEN contact_prefix.contact_without_prefix IS NULL THEN NULL ELSE LTRIM(RTRIM(contact_prefix.contact_without_prefix)) END AS contact_trim_1
) AS contact_stage1
OUTER APPLY (
    SELECT CASE
        WHEN contact_stage1.contact_trim_1 IS NULL THEN NULL
        WHEN LEFT(contact_stage1.contact_trim_1, 1) IN (N'{', N'[', N'"', NCHAR(39)) AND LEN(contact_stage1.contact_trim_1) > 1 THEN SUBSTRING(contact_stage1.contact_trim_1, 2, LEN(contact_stage1.contact_trim_1) - 1)
        WHEN LEFT(contact_stage1.contact_trim_1, 1) IN (N'{', N'[', N'"', NCHAR(39)) THEN ''
        ELSE contact_stage1.contact_trim_1
    END AS contact_strip_lead1
) AS contact_lead1
OUTER APPLY (
    SELECT CASE
        WHEN contact_lead1.contact_strip_lead1 IS NULL THEN NULL
        WHEN LEN(contact_lead1.contact_strip_lead1) > 0 AND RIGHT(contact_lead1.contact_strip_lead1, 1) IN (N'}', N']', N'"', NCHAR(39)) THEN LEFT(contact_lead1.contact_strip_lead1, LEN(contact_lead1.contact_strip_lead1) - 1)
        ELSE contact_lead1.contact_strip_lead1
    END AS contact_strip_trail1
) AS contact_trail1
OUTER APPLY (
    SELECT CASE WHEN contact_trail1.contact_strip_trail1 IS NULL THEN NULL ELSE LTRIM(RTRIM(contact_trail1.contact_strip_trail1)) END AS contact_trim_2
) AS contact_stage2
OUTER APPLY (
    SELECT CASE
        WHEN contact_stage2.contact_trim_2 IS NULL THEN NULL
        WHEN LEFT(contact_stage2.contact_trim_2, 1) IN (N'{', N'[', N'"', NCHAR(39)) AND LEN(contact_stage2.contact_trim_2) > 1 THEN SUBSTRING(contact_stage2.contact_trim_2, 2, LEN(contact_stage2.contact_trim_2) - 1)
        WHEN LEFT(contact_stage2.contact_trim_2, 1) IN (N'{', N'[', N'"', NCHAR(39)) THEN ''
        ELSE contact_stage2.contact_trim_2
    END AS contact_strip_lead2
) AS contact_lead2
OUTER APPLY (
    SELECT CASE
        WHEN contact_lead2.contact_strip_lead2 IS NULL THEN NULL
        WHEN LEN(contact_lead2.contact_strip_lead2) > 0 AND RIGHT(contact_lead2.contact_strip_lead2, 1) IN (N'}', N']', N'"', NCHAR(39)) THEN LEFT(contact_lead2.contact_strip_lead2, LEN(contact_lead2.contact_strip_lead2) - 1)
        ELSE contact_lead2.contact_strip_lead2
    END AS contact_strip_trail2
) AS contact_trail2
OUTER APPLY (
    SELECT CASE WHEN contact_trail2.contact_strip_trail2 IS NULL THEN NULL ELSE REPLACE(REPLACE(REPLACE(contact_trail2.contact_strip_trail2, '  ', ' '), '  ', ' '), '  ', ' ') END AS contact_collapsed
) AS contact_collapse
OUTER APPLY (
    SELECT CASE WHEN contact_collapse.contact_collapsed IS NULL THEN NULL ELSE NULLIF(LTRIM(RTRIM(contact_collapse.contact_collapsed)), '') END AS contact_trim_final
) AS contact_stage_final
OUTER APPLY (
    SELECT CASE
        WHEN contact_stage_final.contact_trim_final IS NULL THEN NULL
        WHEN RIGHT(contact_stage_final.contact_trim_final, 1) IN ('-', ':') THEN NULL
        ELSE contact_stage_final.contact_trim_final
    END AS contact_value
) AS contact_final
OUTER APPLY (SELECT TRY_CONVERT(datetime2(0), tokens.created_date_token) AS created_date) AS created_date_norm
CROSS APPLY (
    SELECT
        patient_id_val.patient_id AS patient_id,
        CASE WHEN mrn_norm.mrn_standardized IS NULL THEN NULL ELSE CAST(mrn_norm.mrn_standardized AS nvarchar(30)) END AS mrn,
        CASE WHEN name_prep.first_name_collapsed IS NULL THEN NULL WHEN LEN(name_prep.first_name_collapsed) <= 2 AND name_prep.first_name_collapsed = UPPER(name_prep.first_name_collapsed) THEN CAST(name_prep.first_name_collapsed AS nvarchar(60)) ELSE CAST(first_title.first_name_title AS nvarchar(60)) END AS first_name,
        CASE WHEN name_prep.last_name_collapsed IS NULL THEN NULL WHEN LEN(name_prep.last_name_collapsed) <= 2 AND name_prep.last_name_collapsed = UPPER(name_prep.last_name_collapsed) THEN CAST(name_prep.last_name_collapsed AS nvarchar(60)) ELSE CAST(last_title.last_name_title AS nvarchar(60)) END AS last_name,
        dob_norm.date_of_birth AS date_of_birth,
        age_norm.age_value AS age,
        gender_norm.gender AS gender,
        blood_norm.blood_group AS blood_group,
        CASE WHEN phone_norm.phone_normalized IS NULL THEN NULL ELSE CAST(phone_norm.phone_normalized AS nvarchar(16)) END AS phone,
        CASE WHEN address_clean.address_collapsed IS NULL THEN NULL ELSE CAST(address_clean.address_collapsed AS nvarchar(250)) END AS address,
        CASE WHEN area_title.area_title_case IS NULL THEN NULL ELSE CAST(area_title.area_title_case AS nvarchar(60)) END AS area,
        pincode_norm.pincode AS pincode,
        CASE WHEN insurance_norm.insurance_type IS NULL THEN NULL ELSE CAST(insurance_norm.insurance_type AS nvarchar(20)) END AS insurance_type,
        CASE WHEN contact_final.contact_value IS NULL THEN NULL ELSE CAST(contact_final.contact_value AS nvarchar(200)) END AS emergency_contact,
        created_date_norm.created_date AS created_date
) AS typed
WHERE typed.patient_id IS NOT NULL;

/* Optional covering indexes once the table is populated */
IF NOT EXISTS (SELECT 1
FROM sys.indexes
WHERE name = 'IX_patients_std_tbl_mrn' AND object_id = OBJECT_ID('stg.patients_std_tbl'))
    CREATE NONCLUSTERED INDEX IX_patients_std_tbl_mrn ON stg.patients_std_tbl (mrn) INCLUDE (first_name, last_name, date_of_birth);

IF NOT EXISTS (SELECT 1
FROM sys.indexes
WHERE name = 'IX_patients_std_tbl_demographics' AND object_id = OBJECT_ID('stg.patients_std_tbl'))
    CREATE NONCLUSTERED INDEX IX_patients_std_tbl_demographics ON stg.patients_std_tbl (last_name, first_name) INCLUDE (date_of_birth, phone, insurance_type);
