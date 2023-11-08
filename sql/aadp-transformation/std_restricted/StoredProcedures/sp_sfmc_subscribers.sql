/****** Object:  StoredProcedure [std_restricted].[sp_sfmc_subscribers]    Script Date: 3/31/2022 7:11:01 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std_restricted].[sp_sfmc_subscribers] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN

    truncate table [std_restricted].[sfmc_subscribers];

    insert into [std_restricted].[sfmc_subscribers]
  
    SELECT DISTINCT CAST([SubscriberID]					    AS    INT) AS SubscriberID,
		[EmailAddress]                
	    ,CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp,
			getDate() AS md_record_written_timestamp,
			@pipelineid AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'SFMC' AS md_source_system
    FROM stage_restricted.[sfmc_subscribers]               
      OPTION (LABEL = 'AADPSTDSFMCSUBR');

	  UPDATE STATISTICS [std_restricted].[sfmc_subscribers];
	  UPDATE STATISTICS stage_restricted.[sfmc_subscribers]; 
	  
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCSUBR'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std_restricted.sfmc_subscribers;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std_restricted.sfmc_subscribers WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std_restricted.sp_sfmc_subscribers' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO

