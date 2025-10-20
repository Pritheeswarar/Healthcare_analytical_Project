-- Mapping between source department codes and canonical department identifiers
IF OBJECT_ID('ref.department_map', 'U') IS NULL
BEGIN
    CREATE TABLE ref.department_map
    (
        source_system sysname NOT NULL,
        source_department_code nvarchar(50) NOT NULL,
        department_id int NOT NULL
    );
END;
-- TODO: populate from public code sets or curated mapping.
