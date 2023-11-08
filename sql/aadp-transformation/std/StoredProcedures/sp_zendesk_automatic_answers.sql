/****** Object:  StoredProcedure [std].[sp_zendesk_automatic_answers]    Script Date: 4/29/2022 6:57:12 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_automatic_answers] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

insert into std.[zendesk_automatic_answers]
SELECT distinct [answer_bot_channel]
      ,[articles]
      ,[brand_id]
      ,[created_at]
      ,[enquiry]
      ,[id]
      ,[solved_article_id]
      ,[state]
      ,[ticket_id]
      ,[updated_at]
      ,[user_id]
      ,[md_record_ingestion_timestamp]
      ,[md_record_ingestion_pipeline_id]
      ,[md_source_system]
      ,getdate() as [md_record_written_timestamp]
	  ,@pipelineid [md_record_written_pipeline_id]
      ,@jobid [md_transformation_job_id]
  FROM [stage].[zendesk_automatic_answers]
  

  	OPTION (LABEL = 'AADPSTDZNDSKAUTOANSWR');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKAUTOANSWR'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table [stage].[zendesk_automatic_answers]

		END

		

		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_automatic_answers;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_automatic_answers where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_zendesk_automatic_answers' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end