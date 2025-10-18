/*
File: sql/00_metadata/02_profile_columns_template.sql
Description: Column profiling template returning per-column statistics with optional sampling.
*/

USE [Healthcare];

DECLARE @schema sysname = 'dbo';
DECLARE @table sysname = 'providers';

DECLARE @qualified_table nvarchar(520) = QUOTENAME(@schema) + '.' + QUOTENAME(@table);
DECLARE @object_id int = OBJECT_ID(@qualified_table);
IF @object_id IS NULL
BEGIN
    RAISERROR('Target object not found: %s.%s', 16, 1, @schema, @table);
    RETURN;
END;

DECLARE @rowcount bigint = (
    SELECT SUM(row_count)
FROM sys.dm_db_partition_stats
WHERE object_id = @object_id
    AND index_id IN (0, 1)
);

DECLARE @sample_source nvarchar(max) = N'SELECT * FROM ' + @qualified_table;
IF ISNULL(@rowcount, 0) > 10000000
BEGIN
    -- Sample to the first million rows to keep runtime manageable on very large tables.
    SET @sample_source = N'SELECT TOP (1000000) * FROM ' + @qualified_table + N' ORDER BY (SELECT NULL)';
END;

IF OBJECT_ID('tempdb..#column_profile') IS NOT NULL DROP TABLE #column_profile;

CREATE TABLE #column_profile
(
    column_name sysname NOT NULL,
    data_type sysname NOT NULL,
    sample_rows bigint NULL,
    null_count bigint NULL,
    null_pct decimal(5, 2) NULL,
    distinct_count bigint NULL,
    min_value nvarchar(4000) NULL,
    max_value nvarchar(4000) NULL,
    sample_values nvarchar(max) NULL
);

DECLARE @numeric_types TABLE (type_name sysname NOT NULL);
INSERT INTO @numeric_types
    (type_name)
VALUES
    ('bigint'),
    ('int'),
    ('smallint'),
    ('tinyint'),
    ('decimal'),
    ('numeric'),
    ('float'),
    ('real'),
    ('money'),
    ('smallmoney'),
    ('bit');

DECLARE @datetime_types TABLE (type_name sysname NOT NULL);
INSERT INTO @datetime_types
    (type_name)
VALUES
    ('date'),
    ('datetime'),
    ('datetime2'),
    ('datetimeoffset'),
    ('smalldatetime'),
    ('time');

DECLARE @columns TABLE
(
    column_id int NOT NULL,
    column_name sysname NOT NULL,
    data_type sysname NOT NULL,
    is_numeric bit NOT NULL,
    is_datetime bit NOT NULL
);

INSERT INTO @columns
    (column_id, column_name, data_type, is_numeric, is_datetime)
SELECT
    c.column_id,
    c.name,
    t.name,
    CASE WHEN EXISTS (SELECT 1
    FROM @numeric_types
    WHERE type_name = t.name) THEN 1 ELSE 0 END,
    CASE WHEN EXISTS (SELECT 1
    FROM @datetime_types
    WHERE type_name = t.name) THEN 1 ELSE 0 END
FROM sys.columns AS c
    INNER JOIN sys.types AS t
    ON c.user_type_id = t.user_type_id
WHERE c.object_id = @object_id
ORDER BY c.column_id;

DECLARE @column_id int;
DECLARE @column_name sysname;
DECLARE @data_type sysname;
DECLARE @is_numeric bit;
DECLARE @is_datetime bit;

DECLARE column_cursor CURSOR FAST_FORWARD FOR
SELECT column_id, column_name, data_type, is_numeric, is_datetime
FROM @columns
ORDER BY column_id;

OPEN column_cursor;
FETCH NEXT FROM column_cursor INTO @column_id, @column_name, @data_type, @is_numeric, @is_datetime;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @col_quoted nvarchar(260) = QUOTENAME(@column_name);
    DECLARE @col_literal nvarchar(520) = '''' + REPLACE(@column_name, '''', '''''') + '''';
    DECLARE @datatype_literal nvarchar(520) = '''' + REPLACE(@data_type, '''', '''''') + '''';

    DECLARE @profile_sql nvarchar(max) = N'WITH sample_data AS (' + @sample_source + N'),
base_stats AS (
    SELECT
        COUNT(*) AS sample_rows,
        SUM(CASE WHEN sd.' + @col_quoted + N' IS NULL THEN 1 ELSE 0 END) AS null_count,
        COUNT(DISTINCT CASE WHEN sd.' + @col_quoted + N' IS NULL THEN NULL ELSE TRY_CONVERT(nvarchar(4000), sd.' + @col_quoted + N') END) AS distinct_count
    FROM sample_data AS sd
),
sample_values AS (
    SELECT STUFF((
        SELECT TOP (5) '', '' + value_repr + '' ('' + CAST(freq AS nvarchar(20)) + '')''
        FROM (
            SELECT
                TRY_CONVERT(nvarchar(4000), sd_inner.' + @col_quoted + N') AS value_repr,
                COUNT(*) AS freq
            FROM sample_data AS sd_inner
            WHERE sd_inner.' + @col_quoted + N' IS NOT NULL
            GROUP BY TRY_CONVERT(nvarchar(4000), sd_inner.' + @col_quoted + N')
        ) AS freq_tbl
        ORDER BY freq_tbl.freq DESC, freq_tbl.value_repr
        FOR XML PATH(''''), TYPE
    ).value(''.'', ''nvarchar(max)''), 1, 2, '''') AS sample_values
)';

    IF @is_numeric = 1
    BEGIN
        SET @profile_sql += N',
range_stats AS (
    SELECT
        CONVERT(nvarchar(4000), MIN(TRY_CONVERT(float, sd.' + @col_quoted + N'))) AS min_value,
        CONVERT(nvarchar(4000), MAX(TRY_CONVERT(float, sd.' + @col_quoted + N'))) AS max_value
    FROM sample_data AS sd
    WHERE sd.' + @col_quoted + N' IS NOT NULL
)';
    END
    ELSE IF @is_datetime = 1
    BEGIN
        SET @profile_sql += N',
range_stats AS (
    SELECT
        CONVERT(nvarchar(4000), MIN(TRY_CONVERT(datetime2(6), sd.' + @col_quoted + N')), 121) AS min_value,
        CONVERT(nvarchar(4000), MAX(TRY_CONVERT(datetime2(6), sd.' + @col_quoted + N')), 121) AS max_value
    FROM sample_data AS sd
    WHERE sd.' + @col_quoted + N' IS NOT NULL
)';
    END
    ELSE
    BEGIN
        SET @profile_sql += N',
range_stats AS (
    SELECT CAST(NULL AS nvarchar(4000)) AS min_value, CAST(NULL AS nvarchar(4000)) AS max_value
)';
    END;

    SET @profile_sql += N'
INSERT INTO #column_profile (column_name, data_type, sample_rows, null_count, null_pct, distinct_count, min_value, max_value, sample_values)
SELECT
    ' + @col_literal + N' AS column_name,
    ' + @datatype_literal + N' AS data_type,
    base.sample_rows,
    base.null_count,
    CASE WHEN base.sample_rows = 0 THEN 0 ELSE CAST(base.null_count * 100.0 / base.sample_rows AS decimal(5, 2)) END AS null_pct,
    base.distinct_count,
    rs.min_value,
    rs.max_value,
    sv.sample_values
FROM base_stats AS base
CROSS JOIN range_stats AS rs
CROSS JOIN sample_values AS sv;';

    EXEC sp_executesql @profile_sql;

    FETCH NEXT FROM column_cursor INTO @column_id, @column_name, @data_type, @is_numeric, @is_datetime;
END;

CLOSE column_cursor;
DEALLOCATE column_cursor;

SELECT *
FROM #column_profile
ORDER BY column_name;
