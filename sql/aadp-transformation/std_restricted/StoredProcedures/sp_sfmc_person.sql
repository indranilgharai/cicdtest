/****** Object:  StoredProcedure [std_restricted].[sp_sfmc_person]    Script Date: 3/31/2022 7:10:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std_restricted].[sp_sfmc_person] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			truncate table [std_restricted].[sfmc_person];

			insert into [std_restricted].[sfmc_person]
			SELECT DISTINCT [person_uuid],
			[email]                              
		    ,CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp,
			getDate() AS md_record_written_timestamp,
			@pipelineid AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'SFMC' AS md_source_system
		  FROM stage_restricted.[sfmc_person]
  				OPTION (LABEL = 'AADPSTDSFMCPRSNR');

				   UPDATE STATISTICS [std_restricted].[sfmc_person];
				   UPDATE STATISTICS stage_restricted.[sfmc_person];

					--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
					DECLARE @label VARCHAR(500)

					SET @label = 'AADPSTDSFMCPRSNR'

					EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std_restricted.sfmc_person;

			SELECT @onlydate = CAST(@newrec AS DATE);

			
			DELETE FROM std_restricted.sfmc_person WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std_restricted.sp_sfmc_person' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO

