SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_tickets] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

insert into [std].[zendesk_tickets]
(
    [assignee_id],
    [brand_id],
    [created_at],
    [generated_timestamp],
    [id],
    [priority],
    [requester_id],
    [status],
    [submitter_id],
    [tags],
    [ticket_form_id],
    [type],
    [updated_at],
    [agent_wait_time_in_minutes_business],
    [agent_wait_time_in_minutes_calendar],
    [assigned_at],
    [assignee_stations],
    [assignee_updated_at],
    [metric_created_at],
    [first_resolution_time_in_minutes_business],
    [first_resolution_time_in_minutes_calendar],
    [full_resolution_time_in_minutes_business],
    [full_resolution_time_in_minutes_calendar],
    [initially_assigned_at],
    [on_hold_time_in_minutes_business],
    [on_hold_time_in_minutes_calendar],
    [reopens],
    [replies],
    [reply_time_in_minutes_business],
    [reply_time_in_minutes_calendar],
    [reply_time_in_seconds_business],
    [reply_time_in_seconds_calendar],
    [requester_updated_at],
    [solved_at],
    [requester_wait_time_in_minutes_business],
    [requester_wait_time_in_minutes_calendar],
    [channel],
    [integration_service_instance_name],
    [registered_integration_service_name],
    [md_record_ingestion_timestamp],
    [md_record_ingestion_pipeline_id],
    [md_source_system],
    [md_record_written_timestamp],
    [md_record_written_pipeline_id],
    [md_transformation_job_id]
)
SELECT distinct 
    [assignee_id],
    [brand_id],
    [created_at],
    [generated_timestamp],
    [id],
    [priority],
    [requester_id],
    [status],
    [submitter_id],
    [tags],
    [ticket_form_id],
    [type],
    [updated_at],
    [agent_wait_time_in_minutes_business],
    [agent_wait_time_in_minutes_calendar],
    [assigned_at],
    [assignee_stations],
    [assignee_updated_at],
    [metric_created_at],
    [first_resolution_time_in_minutes_business],
    [first_resolution_time_in_minutes_calendar],
    [full_resolution_time_in_minutes_business],
    [full_resolution_time_in_minutes_calendar],
    [initially_assigned_at],
    [on_hold_time_in_minutes_business],
    [on_hold_time_in_minutes_calendar],
    [reopens],
    [replies],
    [reply_time_in_minutes_business],
    [reply_time_in_minutes_calendar],
    [reply_time_in_seconds_business],
    [reply_time_in_seconds_calendar],
    [requester_updated_at],
    [solved_at],
    [requester_wait_time_in_minutes_business],
    [requester_wait_time_in_minutes_calendar],
    [channel],
    [integration_service_instance_name],
    [registered_integration_service_name],
    CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
    [md_record_ingestion_pipeline_id],
    [md_source_system],
    getdate() as [md_record_written_timestamp],
    @pipelineid [md_record_written_pipeline_id],
    @jobid [md_transformation_job_id]

  FROM [stage].[zendesk_tickets]

  	OPTION (LABEL = 'AADPSTDZNDSKTKTS');

    WITH latest_tickets AS (
	SELECT
		*,
		rank() OVER (
			PARTITION BY id
			ORDER BY
                [updated_at] DESC,
				[md_record_ingestion_timestamp] DESC,
				md_record_written_timestamp DESC
		) AS dupcnt
	FROM
		[std].[zendesk_tickets]
)
DELETE FROM
	latest_tickets
WHERE
	dupcnt > 1;

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKTKTS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table stage.[zendesk_tickets]

		END

		

		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_tickets;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_tickets where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_zendesk_tickets' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end

  