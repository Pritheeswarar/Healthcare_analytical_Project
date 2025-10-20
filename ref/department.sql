-- Reference table for canonical department metadata
IF OBJECT_ID('ref.department', 'U') IS NULL
BEGIN
    CREATE TABLE ref.department
    (
        department_id int NOT NULL,
        department_name sysname NOT NULL,
        is_active bit NOT NULL DEFAULT (1)
    );
END;
-- TODO: populate from public code sets or curated mapping.
