/*
File: sql/00_metadata/00_info_schema_inventory.sql
Author: Analytics Engineering Team
Description: Column-level inventory sourced from INFORMATION_SCHEMA for current database.
Dependencies: INFORMATION_SCHEMA.COLUMNS
Change Log:
  - 2025-10-18: Initial commit.
*/

USE [Healthcare];

SELECT
    c.TABLE_SCHEMA AS table_schema,
    c.TABLE_NAME AS table_name,
    c.COLUMN_NAME AS column_name,
    c.DATA_TYPE AS data_type,
    c.IS_NULLABLE AS is_nullable,
    c.CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
    c.NUMERIC_PRECISION AS numeric_precision,
    c.NUMERIC_SCALE AS numeric_scale
FROM INFORMATION_SCHEMA.COLUMNS AS c
WHERE c.TABLE_CATALOG = DB_NAME()
ORDER BY
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.ORDINAL_POSITION;
