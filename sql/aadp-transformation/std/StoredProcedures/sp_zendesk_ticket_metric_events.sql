/****** Object:  StoredProcedure [std].[sp_zendesk_ticket_metric_events]    Script Date: 4/29/2022 6:56:13 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_ticket_metric_events] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

insert into std.zendesk_ticket_metric_events
SELECT distinct  [deleted]
      ,[id]
      ,[instance_id]
      ,[metric]
      ,[sla]
      ,[status]
      ,[ticket_id]
      ,[time]
      ,[type]
      ,[md_record_ingestion_timestamp]
      ,[md_record_ingestion_pipeline_id]
      ,[md_source_system]
      ,getdate() as [md_record_written_timestamp]
	  ,@pipelineid [md_record_written_pipeline_id]
      ,@jobid [md_transformation_job_id]
  FROM [stage].[zendesk_ticket_metric_events]
  

  	OPTION (LABEL = 'AADPSTDZNDSKTKTMTRCEVNT');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKTKTMTRCEVNT'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table [stage].[zendesk_ticket_metric_events]

		END

		

		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_ticket_metric_events;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_ticket_metric_events where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_zendesk_ticket_metric_events' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end

  