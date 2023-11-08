/****** Object:  StoredProcedure [std].[sp_sfmc_unsubscribe]    Script Date: 4/12/2022 7:59:46 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_sfmc_unsubscribe] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN


      insert into [std].[sfmc_unsubscribe]
      SELECT  DISTINCT CAST([HashKey]     					   						as varchar(200) )             as [HashKey] 
      ,CAST([SubscriberKey]                     AS    VARCHAR(200)         ) AS  [SubscriberKey]
      ,CAST( [EventDate]						   as datetime) AS [EventDate]  
      , CAST([IsUnique]                            AS  VARCHAR(200)) AS [IsUnique] 
      ,CAST([AccountID]                           AS BIGINT ) AS [AccountID]  
      ,CAST([OYBAccountID]                        AS BIGINT ) AS [OYBAccountID]  
      , CAST([SubscriberID]                        AS BIGINT ) AS [SubscriberID] 
      ,CAST([JobID]                               AS  BIGINT) AS [JobID]   
      ,CAST([ListID]                              AS  BIGINT) AS [ListID]
      ,CAST([BatchID]                             AS  BIGINT) AS [BatchID]     
      , CAST(convert(datetime, [md_record_ingestion_timestamp],103) as datetime)     AS [md_record_ingestion_timestamp]
      , CAST([md_record_ingestion_pipeline_id]     AS  VARCHAR(500)) AS [md_record_ingestion_pipeline_id] 
      , CAST([md_source_system]                    AS  VARCHAR(200)) AS [md_source_system] 
	    ,getdate() as [md_record_written_timestamp]
	    , @pipelineid [md_record_written_pipeline_id] 
	    , @jobid [md_transformation_job_id] 
      FROM stage.[sfmc_unsubscribe]
      OPTION (LABEL = 'AADPSTDSFMCUNSUB');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCUNSUB'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		truncate table stage.[sfmc_unsubscribe];

		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_unsubscribe;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.sfmc_unsubscribe WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_unsubscribe' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END