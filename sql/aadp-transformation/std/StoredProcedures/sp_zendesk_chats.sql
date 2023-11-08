
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_chats] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
insert into [std].[zendesk_chats]
SELECT distinct
	[abandon_time]
      ,[agent_ids]
      ,[agent_names]
      ,[comment]
      ,[conversions]
      ,[count]
      ,[deleted]
      ,[department_id]
      ,[department_name]
      ,[dropped]
      ,[duration]
      ,[end_timestamp]
      ,[engagements]
      ,[history]
      ,[id]
      ,[message]
      ,[missed]
      ,[proactive]
      ,[rating]
      ,[referrer_search_engine]
      ,[referrer_search_terms]
      ,[response_time]
      ,[session]
      ,[skills_fulfilled]
      ,[skills_requested]
      ,[started_by]
      ,[tags]
      ,[timestamp]
      ,[triggered]
      ,[triggered_response]
      ,[type]
      ,[unread]
      ,[update_timestamp]
      ,[visitor]
      ,[webpath]
      ,[zendesk_ticket_id]
      ,[md_record_ingestion_timestamp]
      ,[md_record_ingestion_pipeline_id]
      ,[md_source_system]
      ,getdate() as [md_record_written_timestamp]
	  ,@pipelineid [md_record_written_pipeline_id]
      ,@jobid [md_transformation_job_id]
  FROM [stage].[zendesk_chats]

  	OPTION (LABEL = 'AADPSTDZNDSKCHTS');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKCHTS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table stage.zendesk_chats

		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_chats;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_chats where md_record_written_timestamp=@newrec;
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