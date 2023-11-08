/****** Object:  Stored Procedure [std].[sp_zendesk_legs]    Script Date: 24/10/2022 10:00:59 AM ******/


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_legs] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

insert into [std].[zendesk_legs]

SELECT distinct
    [agent_id],
	[call_id],
	[completion_status],
	[consultation_time],
	[consultation_to],
	[created_at],
	[duration],
	[hold_time],
	[id],
	[quality_issues],
	[talk_time],
	[transferred_from],
	[transferred_to],
	[type],
	[updated_at],
	[user_id],
	[wrap_up_time],
	CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
	[md_record_ingestion_pipeline_id],
	[md_source_system],
    getdate() as [md_record_written_timestamp]
	,@pipelineid [md_record_written_pipeline_id]
    ,@jobid [md_transformation_job_id]
	
  FROM [stage].[zendesk_legs]

  	OPTION (LABEL = 'AADPSTDZNDSKCHTS');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKCHTS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table stage.zendesk_legs

		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_legs;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_legs where md_record_written_timestamp=@newrec;
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