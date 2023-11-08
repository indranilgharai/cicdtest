/****** Object:  StoredProcedure [std].[sp_sfmc_journey]    Script Date: 4/12/2022 7:09:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_sfmc_journey] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN

			INSERT INTO [std].[sfmc_journey]
			SELECT DISTINCT CAST([Hashkey] AS VARCHAR(200)) AS [Hashkey]
				,CAST([VersionID] AS VARCHAR(200)) AS [VersionID]
				,CAST([JourneyID] AS VARCHAR(200)) AS [JourneyID]
				,CAST([JourneyName] AS VARCHAR(200)) AS [JourneyName]
				,CAST([VersionNumber] AS VARCHAR(200)) AS [VersionNumber]
				,CAST([CreatedDate] AS datetime) AS [CreatedDate]
				,CAST([LastPublishedDate] AS datetime) AS [LastPublishedDate]
				,CAST([ModifiedDate] AS datetime) AS [ModifiedDate]
				,CAST([JourneyStatus] AS VARCHAR(200)) AS [JourneyStatus]
				,CAST([AccountID] AS BIGINT) AS [AccountID]
				,CAST(CONVERT(DATETIME, [md_record_ingestion_timestamp], 103) AS DATETIME) AS md_record_ingestion_timestamp
				,CAST([md_record_ingestion_pipeline_id] AS VARCHAR(500)) AS [md_record_ingestion_pipeline_id]
				,CAST([md_source_system] AS VARCHAR(200)) AS [md_source_system]
				,getdate() AS [md_record_written_timestamp]
				,@pipelineid [md_record_written_pipeline_id]
				,@jobid [md_transformation_job_id]
			FROM [stage].[sfmc_journey]

			OPTION (LABEL = 'AADPSTDSFMCJNY');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPSTDSFMCJNY'

			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		
		truncate table [stage].[sfmc_journey];

		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_journey;

			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.sfmc_journey WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_journey' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
