USE [msdb];
GO

DECLARE @job_name sysname = N'Stg Optimized Timing Runner';
IF EXISTS (SELECT 1
FROM msdb.dbo.sysjobs
WHERE name = @job_name)
BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name = @job_name;
END;

DECLARE @script_path nvarchar(4000) = N'C:\Users\sprit\OneDrive\Desktop\Healthcare_SQL_Project\ops\benchmarking\timing_runner.sql';
DECLARE @command nvarchar(4000) = N'sqlcmd -S $(ESCAPE_SQUOTE(SRVR)) -d Healthcare -i "' + @script_path + '" -b -I';

EXEC msdb.dbo.sp_add_job
    @job_name        = @job_name,
    @enabled         = 1,
    @description     = N'Runs timing probes against stg_optimized views and logs metrics to ops.benchmarks.',
    @category_name   = N'[Uncategorized (Local)]';

EXEC msdb.dbo.sp_add_jobstep
    @job_name      = @job_name,
    @step_name     = N'Run timing runner',
    @subsystem     = N'CmdExec',
    @command       = @command,
    @retry_attempts = 0,
    @on_success_action = 1,
    @on_fail_action    = 2;

EXEC msdb.dbo.sp_add_schedule
    @schedule_name = N'Stg Optimized Timing Runner - Daily 02:00',
    @freq_type     = 4,   -- daily
    @freq_interval = 1,
    @freq_subday_type = 1,
    @freq_subday_interval = 0,
    @active_start_time = 20000;

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = @job_name,
    @schedule_name = N'Stg Optimized Timing Runner - Daily 02:00';

EXEC msdb.dbo.sp_add_jobserver
    @job_name   = @job_name,
    @server_name = @@SERVERNAME;

PRINT 'Agent job "' + @job_name + '" created. Update @script_path if repository location changes.';
