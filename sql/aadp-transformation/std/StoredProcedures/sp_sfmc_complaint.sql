/****** Object:  StoredProcedure [std].[sp_sfmc_complaint]    Script Date: 4/12/2022 5:50:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_sfmc_complaint] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
		INSERT INTO [std].[sfmc_complaint]
	  SELECT DISTINCT CAST([HashKey] AS VARCHAR(200)) AS [HashKey]
		,CAST([AccountID] AS bigint) AS [AccountID]
		,CAST([OYBAccountID] AS bigint) AS [OYBAccountID]
		,CAST([JobID] AS bigint) AS [JobID]
		,CAST([ListID] AS bigint) AS [ListID]
		,CAST([BatchID] AS bigint) AS [BatchID]
		,CAST([SubscriberID] AS bigint) AS [SubscriberID]
		,CAST([SubscriberKey] AS VARCHAR(200)) AS [SubscriberKey]
		,CAST([EventDate] AS DATETIME) AS [EventDate]
		,CAST([IsUnique] AS VARCHAR(200)) AS [IsUnique]
		,CAST([Domain] AS VARCHAR(200)) AS [Domain]
		,CAST(convert(DATETIME, [md_record_ingestion_timestamp], 103) AS DATETIME) AS [md_record_ingestion_timestamp]
		,CAST([md_record_ingestion_pipeline_id] AS VARCHAR(500)) AS [md_record_ingestion_pipeline_id]
		,CAST([md_source_system] AS VARCHAR(200)) AS [md_source_system]
		,getdate() AS [md_record_written_timestamp]
		,@pipelineid [md_record_written_pipeline_id]
		,@jobid [md_transformation_job_id]
	  FROM [stage].[sfmc_complaint]
	
	  
	  OPTION (LABEL = 'AADPSTDSFMCCMP');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label = 'AADPSTDSFMCCMP'
      
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		
		truncate table [stage].[sfmc_complaint];

		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_complaint;

			SELECT @onlydate = CAST(@newrec AS DATE);

			
			DELETE FROM std.sfmc_complaint WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_complaint' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END