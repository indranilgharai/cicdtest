SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [meta_ctl].[sp_row_count] @jobid [int],@step_number [int],@label [nvarchar](100) AS 
	BEGIN
	DECLARE @driver_read_count int;
	SELECT @driver_read_count=s.row_count
	FROM sys.dm_pdw_request_steps s, sys.dm_pdw_exec_requests r
	Where r.request_id = s.request_id 
	and s.row_count > -1
	and r.[label] = @label	
	and s.command like 'SELECT%'
	order by r.end_time desc;

	DECLARE @target_write_count int;
	SELECT @target_write_count=s.row_count
	FROM sys.dm_pdw_request_steps s, sys.dm_pdw_exec_requests r
	Where r.request_id = s.request_id 
	and s.row_count > -1
	and r.[label] = @label
	and s.operation_type<>'ShuffleMoveOperation'
	and s.command not like 'SELECT%'
	order by r.end_time desc;

	insert [meta_ctl].[transform_count_record_table] select (select @jobid),(select @step_number),(select @driver_read_count),(select @target_write_count),(select getdate());
	END
