/*
    Source table: dbo.billing
    Transformed columns: bill_id, admission_id, bill_date, payment_date, payment_status,
        doctor_fees, insurance_coverage, lab_charges, nursing_charges,
        pharmacy_charges, procedure_charges, room_charges, total_amount, patient_due
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

CREATE OR ALTER VIEW stg_optimized.billing_std
AS
    WITH
        source
        AS
        (
            SELECT
                bill_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.bill_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                admission_id_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.admission_id), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                bill_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.bill_date), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                payment_date_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.payment_date), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                payment_status_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.payment_status), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                doctor_fees_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.doctor_fees), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                insurance_coverage_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.insurance_coverage), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                lab_charges_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.lab_charges), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                nursing_charges_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.nursing_charges), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                pharmacy_charges_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.pharmacy_charges), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                procedure_charges_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.procedure_charges), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                room_charges_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.room_charges), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                total_amount_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.total_amount), CHAR(9) + CHAR(10) + CHAR(13), '   ')),
                patient_due_text = TRIM(TRANSLATE(CONVERT(nvarchar(255), b.patient_due), CHAR(9) + CHAR(10) + CHAR(13), '   '))
            FROM dbo.billing AS b
        ),
        tokens
        AS
        (
            SELECT
                bill_id_token = CASE
            WHEN bill_id_text IS NULL OR bill_id_text = ''
                    OR UPPER(bill_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(bill_id_text, ',', '')
        END,
                admission_id_token = CASE
            WHEN admission_id_text IS NULL OR admission_id_text = ''
                    OR UPPER(admission_id_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(admission_id_text, ',', '')
        END,
                bill_date_token = CASE
            WHEN bill_date_text IS NULL OR bill_date_text = ''
                    OR UPPER(bill_date_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(bill_date_text, '/', '-'), '.', '-')
        END,
                payment_date_token = CASE
            WHEN payment_date_text IS NULL OR payment_date_text = ''
                    OR UPPER(payment_date_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(payment_date_text, '/', '-'), '.', '-')
        END,
                payment_status_token = CASE
            WHEN payment_status_text IS NULL OR payment_status_text = ''
                    OR UPPER(payment_status_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE payment_status_text
        END,
                doctor_fees_token = CASE
            WHEN doctor_fees_text IS NULL OR doctor_fees_text = ''
                    OR UPPER(doctor_fees_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(doctor_fees_text, ',', ''), ' ', '')
        END,
                insurance_coverage_token = CASE
            WHEN insurance_coverage_text IS NULL OR insurance_coverage_text = ''
                    OR UPPER(insurance_coverage_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(insurance_coverage_text, ',', ''), ' ', '')
        END,
                lab_charges_token = CASE
            WHEN lab_charges_text IS NULL OR lab_charges_text = ''
                    OR UPPER(lab_charges_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(lab_charges_text, ',', ''), ' ', '')
        END,
                nursing_charges_token = CASE
            WHEN nursing_charges_text IS NULL OR nursing_charges_text = ''
                    OR UPPER(nursing_charges_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(nursing_charges_text, ',', ''), ' ', '')
        END,
                pharmacy_charges_token = CASE
            WHEN pharmacy_charges_text IS NULL OR pharmacy_charges_text = ''
                    OR UPPER(pharmacy_charges_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(pharmacy_charges_text, ',', ''), ' ', '')
        END,
                procedure_charges_token = CASE
            WHEN procedure_charges_text IS NULL OR procedure_charges_text = ''
                    OR UPPER(procedure_charges_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(procedure_charges_text, ',', ''), ' ', '')
        END,
                room_charges_token = CASE
            WHEN room_charges_text IS NULL OR room_charges_text = ''
                    OR UPPER(room_charges_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(room_charges_text, ',', ''), ' ', '')
        END,
                total_amount_token = CASE
            WHEN total_amount_text IS NULL OR total_amount_text = ''
                    OR UPPER(total_amount_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(total_amount_text, ',', ''), ' ', '')
        END,
                patient_due_token = CASE
            WHEN patient_due_text IS NULL OR patient_due_text = ''
                    OR UPPER(patient_due_text) IN ('NULL', 'N/A', 'TBD', '-')
            THEN NULL
            ELSE REPLACE(REPLACE(patient_due_text, ',', ''), ' ', '')
        END
            FROM source
        ),
        typed
        AS
        (
            SELECT
                bill_id = TRY_CONVERT(int, bill_id_token),
                admission_id = TRY_CONVERT(int, admission_id_token),
                bill_date = COALESCE(
            TRY_CONVERT(date, bill_date_token, 23),
            TRY_CONVERT(date, bill_date_token, 120),
            TRY_CONVERT(date, bill_date_token, 121),
            TRY_CONVERT(date, bill_date_token, 111),
            TRY_CONVERT(date, bill_date_token, 112),
            TRY_CONVERT(date, bill_date_token, 101),
            TRY_CONVERT(date, bill_date_token, 103),
            TRY_CONVERT(date, bill_date_token, 105),
            TRY_CONVERT(date, bill_date_token, 107)
        ),
                payment_date = COALESCE(
            TRY_CONVERT(date, payment_date_token, 23),
            TRY_CONVERT(date, payment_date_token, 120),
            TRY_CONVERT(date, payment_date_token, 121),
            TRY_CONVERT(date, payment_date_token, 111),
            TRY_CONVERT(date, payment_date_token, 112),
            TRY_CONVERT(date, payment_date_token, 101),
            TRY_CONVERT(date, payment_date_token, 103),
            TRY_CONVERT(date, payment_date_token, 105),
            TRY_CONVERT(date, payment_date_token, 107)
        ),
                payment_status = CASE
            WHEN payment_status_token IS NULL THEN NULL
            ELSE CASE UPPER(payment_status_token)
                WHEN 'PAID' THEN N'Paid'
                WHEN 'PENDING' THEN N'Pending'
                WHEN 'PARTIAL' THEN N'Partial'
                WHEN 'OUTSTANDING' THEN N'Outstanding'
                WHEN 'WAIVED' THEN N'Waived'
                ELSE NULL
            END
        END,
                doctor_fees = TRY_CONVERT(decimal(18, 2), doctor_fees_token),
                insurance_coverage = TRY_CONVERT(decimal(18, 2), insurance_coverage_token),
                lab_charges = TRY_CONVERT(decimal(18, 2), lab_charges_token),
                nursing_charges = TRY_CONVERT(decimal(18, 2), nursing_charges_token),
                pharmacy_charges = TRY_CONVERT(decimal(18, 2), pharmacy_charges_token),
                procedure_charges = TRY_CONVERT(decimal(18, 2), procedure_charges_token),
                room_charges = TRY_CONVERT(decimal(18, 2), room_charges_token),
                total_amount = TRY_CONVERT(decimal(18, 2), total_amount_token),
                patient_due = TRY_CONVERT(decimal(18, 2), patient_due_token)
            FROM tokens
        )
    SELECT
        bill_id,
        admission_id,
        bill_date,
        payment_date,
        payment_status,
        doctor_fees,
        insurance_coverage,
        lab_charges,
        nursing_charges,
        pharmacy_charges,
        procedure_charges,
        room_charges,
        total_amount,
        patient_due
    FROM typed
    WHERE bill_id IS NOT NULL;
GO

-- Quality gates:
-- Confirm: no TRY_PARSE, no FORMAT, no CROSS APPLY.
-- Confirm: numeric conversions use TRY_CONVERT with decimal(18,2).
-- Confirm: only dbo sources referenced.
-- Smoke test:
-- SELECT TOP (25) * FROM stg_optimized.billing_std ORDER BY bill_id;
