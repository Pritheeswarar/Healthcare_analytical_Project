-- CPT code metadata and normalization reference
IF OBJECT_ID('ref.cpt_map', 'U') IS NULL
BEGIN
    CREATE TABLE ref.cpt_map
    (
        cpt_code nvarchar(10) NOT NULL,
        cpt_description nvarchar(255) NULL,
        effective_date date NULL,
        expiration_date date NULL
    );
END;
-- TODO: populate from public code sets or curated mapping.
