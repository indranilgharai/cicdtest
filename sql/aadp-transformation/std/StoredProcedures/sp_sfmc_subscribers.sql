/****** Object:  StoredProcedure [std].[sp_sfmc_subscribers]    Script Date: 4/12/2022 7:39:14 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_sfmc_subscribers] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN

    truncate table [std].[sfmc_subscribers];

    insert into [std].[sfmc_subscribers]
  
    SELECT DISTINCT CAST([SubscriberID]					    AS    bigint) AS SubscriberID
      ,CAST([DateUndeliverable]                 AS    DATETIME) AS [DateUndeliverable]               
      ,CAST([DateJoined]                        AS    DATETIME ) AS          [DateJoined]                      
      ,CAST([DateUnsubscribed]                  AS    DATETIME         ) AS  [DateUnsubscribed]                
      ,CAST([BounceCount]                       AS    bigint         ) AS  [BounceCount]                     
      ,CAST([SubscriberKey]                     AS    VARCHAR(200)         ) AS  [SubscriberKey]                   
      ,CAST([SubscriberType]                    AS    VARCHAR(200)         ) AS  [SubscriberType]                  
      ,CAST([Status]                            AS    VARCHAR(200)         ) AS  [Status]                          
      ,CAST([Locale]                            AS    VARCHAR(200)         ) AS  [Locale]                          
      ,CAST([brand]                             AS    VARCHAR(200)         ) AS  [brand]                           
      ,CAST([aesop_country]                     AS    VARCHAR(200)         ) AS  [aesop_country]                   
      ,CAST([aesop_language]                    AS    VARCHAR(200)         ) AS  [aesop_language]                  
      ,CAST(CONVERT(DATETIME,[md_record_ingestion_timestamp],103)     AS    DATETIME         ) AS  [md_record_ingestion_timestamp]   
      ,CAST([md_record_ingestion_pipeline_id]   AS    VARCHAR(500)         ) AS  [md_record_ingestion_pipeline_id] 
      ,CAST([md_source_system]                  AS    VARCHAR(200)         ) AS  [md_source_system]                
      ,CAST([Domain]                            AS    VARCHAR(200)         ) AS  [Domain]                          
      ,CAST([EmailAddress]                      AS    VARCHAR(200)         ) AS  [EmailAddress]                    
	    ,getdate() as [md_record_written_timestamp]
	    , @pipelineid [md_record_written_pipeline_id] 
	    , @jobid [md_transformation_job_id] 
    FROM stage.[sfmc_subscribers]   
	where CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME)
			in (select max(CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME)) from stage.[sfmc_subscribers] )
      OPTION (LABEL = 'AADPSTDSFMCSUB');

	  UPDATE STATISTICS [std].[sfmc_subscribers];
	  UPDATE STATISTICS [stage].[sfmc_subscribers];
	  
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCSUB'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_subscribers;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.sfmc_subscribers WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_subscribers' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END

