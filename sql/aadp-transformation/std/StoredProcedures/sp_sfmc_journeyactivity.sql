/****** Object:  StoredProcedure [std].[sp_sfmc_journeyactivity]    Script Date: 4/12/2022 7:13:22 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_sfmc_journeyactivity] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN

			INSERT INTO [std].[sfmc_journeyactivity]
			SELECT DISTINCT CAST([Hashkey] AS VARCHAR(200)) AS [Hashkey]
				,CAST([VersionID] AS VARCHAR(200)) AS [VersionID]
				,CAST([ActivityID] AS VARCHAR(200)) AS [ActivityID]
				,CAST([ActivityName] AS VARCHAR(200)) AS [ActivityName]
				,CAST([ActivityExternalKey] AS VARCHAR(200)) AS [ActivityExternalKey]
				,CAST([JourneyActivityObjectID] AS VARCHAR(200)) AS [JourneyActivityObjectID]
				,CAST([ActivityType] AS VARCHAR(200)) AS [ActivityType]
				,CAST([AccountID] AS BIGINT) AS [AccountID]
				,CAST(CONVERT(DATETIME, [md_record_ingestion_timestamp], 103) AS DATETIME) AS [md_record_ingestion_timestamp]
				,CAST([md_record_ingestion_pipeline_id] AS VARCHAR(500)) AS [md_record_ingestion_pipeline_id]
				,CAST([md_source_system] AS VARCHAR(200)) AS [md_source_system]
				,getdate() AS [md_record_written_timestamp]
				,@pipelineid [md_record_written_pipeline_id]
				,@jobid [md_transformation_job_id]
			FROM STAGE.[sfmc_journeyactivity]
			OPTION (LABEL = 'AADPSTDSFMCJNYACT');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPSTDSFMCJNYACT'

			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

		truncate table STAGE.[sfmc_journeyactivity];

		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_journeyactivity;

			SELECT @onlydate = CAST(@newrec AS DATE);
      
			DELETE FROM std.sfmc_journeyactivity WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_journeyactivity' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO