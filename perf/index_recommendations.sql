USE [Healthcare];
GO

/*
    Nonclustered index recommendations once the staging layer is materialized.
    Current staging objects are views without SCHEMABINDING, so SQL Server
    rejects CREATE INDEX statements. After migrating to persisted tables or
    indexed views, enable the definitions below.
*/

-- Admissions staging table (future) filters on admission_id, patient_id, admission_date
--CREATE INDEX IX_stg_admissions_std_admission_id
--ON stg.admissions_std_tbl (admission_id)
--INCLUDE (patient_id, admission_date, discharge_date);

-- Patients staging table filters on patient_id, phone
--CREATE INDEX IX_stg_patients_std_patient_id
--ON stg.patients_std_tbl (patient_id)
--INCLUDE (mrn, first_name, last_name, phone);

-- Billing staging table filters on payment_status, admission_id
--CREATE INDEX IX_stg_billing_std_payment_status
--ON stg.billing_std_tbl (payment_status, admission_id)
--INCLUDE (total_amount, payment_date);

-- Diagnoses staging table filters on icd_code, admission_id, diagnosis_date
--CREATE INDEX IX_stg_diagnoses_std_code
--ON stg.diagnoses_std_tbl (icd_code, admission_id)
--INCLUDE (diagnosis_description, diagnosis_date);

-- Procedures staging table filters on cpt_code, admission_id, procedure_date
--CREATE INDEX IX_stg_procedures_std_cpt
--ON stg.procedures_std_tbl (cpt_code, admission_id)
--INCLUDE (procedure_date, procedure_cost);

-- Lab results staging table filters on test_name, admission_id, test_date
--CREATE INDEX IX_stg_lab_results_std_test_name
--ON stg.lab_results_std_tbl (test_name, admission_id)
--INCLUDE (test_date, result_value);
