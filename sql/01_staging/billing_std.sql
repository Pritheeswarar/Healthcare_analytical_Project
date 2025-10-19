USE [Healthcare]
GO

IF NOT EXISTS (SELECT 1
FROM sys.schemas
WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

CREATE OR ALTER VIEW stg.billing_std
AS
    /*
    Staging view over dbo.billing applying datatype standardization only.
    Placeholders collapse to NULL; TRY_CONVERT ensures safe typing for identifiers, dates, and currency amounts.
*/
    SELECT
        typed.bill_id,
        typed.admission_id,
        typed.bill_date,
        typed.payment_date,
        typed.payment_status,
        typed.doctor_fees,
        typed.insurance_coverage,
        typed.lab_charges,
        typed.nursing_charges,
        typed.pharmacy_charges,
        typed.procedure_charges,
        typed.room_charges,
        typed.total_amount,
        typed.patient_due
    FROM dbo.billing AS b
CROSS APPLY (
    SELECT
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.bill_id))), '') AS bill_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.admission_id))), '') AS admission_id_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.bill_date))), '') AS bill_date_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.payment_date))), '') AS payment_date_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.payment_status))), '') AS payment_status_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.doctor_fees))), '') AS doctor_fees_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.insurance_coverage))), '') AS insurance_coverage_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.lab_charges))), '') AS lab_charges_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.nursing_charges))), '') AS nursing_charges_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.pharmacy_charges))), '') AS pharmacy_charges_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.procedure_charges))), '') AS procedure_charges_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.room_charges))), '') AS room_charges_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.total_amount))), '') AS total_amount_raw,
            NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(255), b.patient_due))), '') AS patient_due_raw
) AS trimmed
CROSS APPLY (
    SELECT
            CASE WHEN trimmed.bill_id_raw IS NULL OR UPPER(trimmed.bill_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.bill_id_raw, ',', '') END AS bill_id_token,
            CASE WHEN trimmed.admission_id_raw IS NULL OR UPPER(trimmed.admission_id_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(trimmed.admission_id_raw, ',', '') END AS admission_id_token,
            CASE WHEN trimmed.bill_date_raw IS NULL OR UPPER(trimmed.bill_date_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.bill_date_raw, '/', '-'), '.', '-') END AS bill_date_token,
            CASE WHEN trimmed.payment_date_raw IS NULL OR UPPER(trimmed.payment_date_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.payment_date_raw, '/', '-'), '.', '-') END AS payment_date_token,
            CASE WHEN trimmed.payment_status_raw IS NULL OR UPPER(trimmed.payment_status_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE trimmed.payment_status_raw END AS payment_status_token,
            CASE WHEN trimmed.doctor_fees_raw IS NULL OR UPPER(trimmed.doctor_fees_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.doctor_fees_raw, ',', ''), ' ', '') END AS doctor_fees_token,
            CASE WHEN trimmed.insurance_coverage_raw IS NULL OR UPPER(trimmed.insurance_coverage_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.insurance_coverage_raw, ',', ''), ' ', '') END AS insurance_coverage_token,
            CASE WHEN trimmed.lab_charges_raw IS NULL OR UPPER(trimmed.lab_charges_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.lab_charges_raw, ',', ''), ' ', '') END AS lab_charges_token,
            CASE WHEN trimmed.nursing_charges_raw IS NULL OR UPPER(trimmed.nursing_charges_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.nursing_charges_raw, ',', ''), ' ', '') END AS nursing_charges_token,
            CASE WHEN trimmed.pharmacy_charges_raw IS NULL OR UPPER(trimmed.pharmacy_charges_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.pharmacy_charges_raw, ',', ''), ' ', '') END AS pharmacy_charges_token,
            CASE WHEN trimmed.procedure_charges_raw IS NULL OR UPPER(trimmed.procedure_charges_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.procedure_charges_raw, ',', ''), ' ', '') END AS procedure_charges_token,
            CASE WHEN trimmed.room_charges_raw IS NULL OR UPPER(trimmed.room_charges_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.room_charges_raw, ',', ''), ' ', '') END AS room_charges_token,
            CASE WHEN trimmed.total_amount_raw IS NULL OR UPPER(trimmed.total_amount_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.total_amount_raw, ',', ''), ' ', '') END AS total_amount_token,
            CASE WHEN trimmed.patient_due_raw IS NULL OR UPPER(trimmed.patient_due_raw) IN ('NULL', 'N/A', 'TBD', '-')
             THEN NULL ELSE REPLACE(REPLACE(trimmed.patient_due_raw, ',', ''), ' ', '') END AS patient_due_token
) AS tokens
CROSS APPLY (
    SELECT
            TRY_CONVERT(int, tokens.bill_id_token) AS bill_id,
            TRY_CONVERT(int, tokens.admission_id_token) AS admission_id,
            COALESCE(
            TRY_CONVERT(date, tokens.bill_date_token, 23),
            TRY_CONVERT(date, tokens.bill_date_token, 120),
            TRY_CONVERT(date, tokens.bill_date_token, 121),
            TRY_CONVERT(date, tokens.bill_date_token, 111),
            TRY_CONVERT(date, tokens.bill_date_token, 112),
            TRY_CONVERT(date, tokens.bill_date_token, 101),
            TRY_CONVERT(date, tokens.bill_date_token, 103),
            TRY_CONVERT(date, tokens.bill_date_token, 105),
            TRY_CONVERT(date, tokens.bill_date_token, 107),
            TRY_PARSE(tokens.bill_date_token AS date USING 'en-US'),
            TRY_PARSE(tokens.bill_date_token AS date USING 'en-GB')
        ) AS bill_date,
            COALESCE(
            TRY_CONVERT(date, tokens.payment_date_token, 23),
            TRY_CONVERT(date, tokens.payment_date_token, 120),
            TRY_CONVERT(date, tokens.payment_date_token, 121),
            TRY_CONVERT(date, tokens.payment_date_token, 111),
            TRY_CONVERT(date, tokens.payment_date_token, 112),
            TRY_CONVERT(date, tokens.payment_date_token, 101),
            TRY_CONVERT(date, tokens.payment_date_token, 103),
            TRY_CONVERT(date, tokens.payment_date_token, 105),
            TRY_CONVERT(date, tokens.payment_date_token, 107),
            TRY_PARSE(tokens.payment_date_token AS date USING 'en-US'),
            TRY_PARSE(tokens.payment_date_token AS date USING 'en-GB')
        ) AS payment_date,
            CASE
            WHEN tokens.payment_status_token IS NULL THEN NULL
            ELSE CASE UPPER(tokens.payment_status_token)
                WHEN 'PAID' THEN N'Paid'
                WHEN 'PENDING' THEN N'Pending'
                WHEN 'PARTIAL' THEN N'Partial'
                WHEN 'OUTSTANDING' THEN N'Outstanding'
                WHEN 'WAIVED' THEN N'Waived'
                ELSE NULL
            END
        END AS payment_status,
            TRY_CONVERT(decimal(18,2), tokens.doctor_fees_token) AS doctor_fees,
            TRY_CONVERT(decimal(18,2), tokens.insurance_coverage_token) AS insurance_coverage,
            TRY_CONVERT(decimal(18,2), tokens.lab_charges_token) AS lab_charges,
            TRY_CONVERT(decimal(18,2), tokens.nursing_charges_token) AS nursing_charges,
            TRY_CONVERT(decimal(18,2), tokens.pharmacy_charges_token) AS pharmacy_charges,
            TRY_CONVERT(decimal(18,2), tokens.procedure_charges_token) AS procedure_charges,
            TRY_CONVERT(decimal(18,2), tokens.room_charges_token) AS room_charges,
            TRY_CONVERT(decimal(18,2), tokens.total_amount_token) AS total_amount,
            TRY_CONVERT(decimal(18,2), tokens.patient_due_token) AS patient_due
) AS typed
    WHERE typed.bill_id IS NOT NULL;
GO
