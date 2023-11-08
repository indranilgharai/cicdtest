/****** Object:  Stored Procedure [std].[sp_zendesk_calls]    Script Date: 24/10/2022 10:00:59 AM ******/


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_calls] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

Insert into [std].[zendesk_calls]

SELECT distinct
    [agent_id],
	[completion_status],
	[consultation_time],
	[created_at],
	[customer_requested_voicemail],
	[default_group],
	[direction],
	[duration],
	[exceeded_queue_time],
	[hold_time],
	[id],
	[ivr_action],
	[ivr_destination_group_name],
	[ivr_hops],
	[ivr_routed_to],
	[ivr_time_spent],
	[outside_business_hours],
	[quality_issues],
	[talk_time],
	[ticket_id],
	[time_to_answer],
	[updated_at],
	[voicemail],
	[wait_time],
	[wrap_up_time],
	CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
	[md_record_ingestion_pipeline_id],
	[md_source_system],
    getdate() as [md_record_written_timestamp]
	,@pipelineid [md_record_written_pipeline_id]
    ,@jobid [md_transformation_job_id]
	
  FROM [stage].[zendesk_calls]

  	OPTION (LABEL = 'AADPSTDZNDSKCHTS');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKCHTS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table stage.zendesk_calls

		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_calls;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_calls where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_zendesk_chats' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end