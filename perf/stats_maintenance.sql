USE [Healthcare];
GO

/*
    Statistics maintenance guidance for staging schema.
*/

-- Confirm database options
SELECT name AS database_name,
    is_auto_create_stats_on,
    is_auto_update_stats_on,
    is_auto_update_stats_async_on
FROM sys.databases
WHERE name = 'Healthcare';

-- Refresh statistics for high-volatility staging base tables
EXEC sp_updatestats;

-- Fullscan update for critical tables
UPDATE STATISTICS dbo.patients WITH FULLSCAN;
UPDATE STATISTICS dbo.admissions WITH FULLSCAN;
UPDATE STATISTICS dbo.billing WITH FULLSCAN;
UPDATE STATISTICS dbo.diagnoses WITH FULLSCAN;
UPDATE STATISTICS dbo.procedures WITH FULLSCAN;
UPDATE STATISTICS dbo.lab_results WITH FULLSCAN;

-- Optional: schedule weekly job to run sp_updatestats with sampling after daily loads
