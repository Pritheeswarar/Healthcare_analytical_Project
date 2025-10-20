-- Mapping between source payer names and standardized insurance classes
IF OBJECT_ID('ref.insurance_map', 'U') IS NULL
BEGIN
    CREATE TABLE ref.insurance_map
    (
        source_payer_name nvarchar(100) NOT NULL,
        payer_group nvarchar(50) NOT NULL,
        network_status nvarchar(50) NULL
    );
END;
-- TODO: populate from public code sets or curated mapping.
