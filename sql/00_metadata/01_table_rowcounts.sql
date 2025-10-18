/*
File: sql/00_metadata/01_table_rowcounts.sql
Author: Analytics Engineering Team
Description: Table-level row count summary using partition stats.
Dependencies: sys.schemas, sys.tables, sys.dm_db_partition_stats
Change Log:
  - 2025-10-18: Initial commit.
*/

USE [Healthcare];

-- Caution: row counts on heap tables are approximate because sys.dm_db_partition_stats reports estimates for heap partitions.
WITH
    base
    AS
    (
        SELECT
            sch.name AS table_schema,
            tbl.name AS table_name,
            SUM(ps.row_count) AS row_count
        FROM sys.tables AS tbl
            INNER JOIN sys.schemas AS sch
            ON tbl.schema_id = sch.schema_id
            INNER JOIN sys.dm_db_partition_stats AS ps
            ON tbl.object_id = ps.object_id
        WHERE ps.index_id IN (0, 1) -- include heaps (0) and clustered indexes (1)
            AND sch.name NOT IN ('sys', 'INFORMATION_SCHEMA')
        GROUP BY sch.name, tbl.name
    )
SELECT
    table_schema,
    table_name,
    row_count
FROM base
ORDER BY
    table_schema,
    table_name;
