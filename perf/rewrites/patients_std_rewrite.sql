USE [Healthcare];
GO

/*
    Rewrite sketch for stg.patients_std.
    Goals: reuse persisted numbers table, drop TRY_PARSE, ensure typed columns before predicates.
*/

IF OBJECT_ID('tempdb..#PatientsStd') IS NOT NULL
    DROP TABLE #PatientsStd;

SELECT
    p.patient_id,
    p.mrn,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.age,
    p.gender,
    p.blood_group,
    p.phone,
    p.address,
    p.area,
    p.pincode,
    p.insurance_type,
    p.emergency_contact,
    p.created_date
INTO #PatientsStd
FROM dbo.patients_std_materialized AS p;
-- placeholder target after materialization

SELECT TOP (100)
    *
FROM #PatientsStd;
