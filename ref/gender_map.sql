-- Mapping between source gender values and standardized labels
IF OBJECT_ID('ref.gender_map', 'U') IS NULL
BEGIN
    CREATE TABLE ref.gender_map
    (
        source_gender nvarchar(50) NOT NULL,
        standard_gender nvarchar(20) NOT NULL
    );
END;
-- TODO: populate from public code sets or curated mapping.
