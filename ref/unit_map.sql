-- Mapping between source unit identifiers and standardized department units
IF OBJECT_ID('ref.unit_map', 'U') IS NULL
BEGIN
    CREATE TABLE ref.unit_map
    (
        source_unit_code nvarchar(50) NOT NULL,
        unit_name nvarchar(100) NOT NULL,
        unit_type nvarchar(50) NULL
    );
END;
-- TODO: populate from public code sets or curated mapping.
