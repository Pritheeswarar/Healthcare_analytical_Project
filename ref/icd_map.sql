-- ICD code metadata and normalization reference
IF OBJECT_ID('ref.icd_map', 'U') IS NULL
BEGIN
    CREATE TABLE ref.icd_map
    (
        icd_code nvarchar(10) NOT NULL,
        icd_description nvarchar(255) NULL,
        chapter nvarchar(100) NULL,
        effective_date date NULL,
        expiration_date date NULL
    );
END;
-- TODO: populate from public code sets or curated mapping.
