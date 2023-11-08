
/****** Object:  StoredProcedure [std].[sp_sfmc_bounce]    Script Date: 4/12/2022 4:25:33 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create PROC [std].[sp_sfmc_bounce] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			insert into  std.sfmc_bounce
		SELECT  DISTINCT CAST([HashKey]                             AS VARCHAR(200) ) AS [HashKey]                          
		,CAST([SubscriberKey]                       AS VARCHAR(200) ) AS [SubscriberKey]                    
		,CAST([SubscriberID]                        AS BIGINT ) AS [SubscriberID]                     
		,CAST([AccountID]                           AS BIGINT ) AS [AccountID]                        
		,CAST([OYBAccountID]                        AS BIGINT ) AS [OYBAccountID]                     
		,CAST([Domain]                              AS VARCHAR(200)) AS [Domain]                           
		,CAST( [EventDate]						   as datetime) AS [EventDate]     
		,CAST([JobID]                               AS  BIGINT) AS [JobID]                            
		,CAST([ListID]                              AS  BIGINT) AS [ListID]                           
		,CAST([BatchID]                             AS  BIGINT) AS [BatchID]                          
		,CAST([IsUnique]                            AS  VARCHAR(200)) AS [IsUnique]                         
		,CAST([TriggererSendDefinitionObjectID]     AS  VARCHAR(200)) AS [TriggererSendDefinitionObjectID]  
		,CAST([TriggeredSendCustomerKey]            AS  VARCHAR(200)) AS [TriggeredSendCustomerKey]         
		,CAST([BounceTypeID]                        AS  BIGINT) AS [BounceTypeID]                     
		,CAST([BounceType]                          AS  VARCHAR(200)) AS [BounceType]                       
		,CAST([BounceCategoryID]                    AS  BIGINT) AS [BounceCategoryID]                 
		,CAST([BounceCategory]                      AS  VARCHAR(200)) AS [BounceCategory]                   
		,CAST([BounceSubcategoryID]                 AS  BIGINT) AS [BounceSubcategoryID]              
		,CAST([BounceSubcategory]                   AS  VARCHAR(200)) AS [BounceSubcategory]                
		,CAST([Reason]                              AS  VARCHAR(200)) AS [Reason]                           
		,CAST([SMTPCode]                            AS  BIGINT) AS [SMTPCode]                         
		,CAST([EnhancedStatusCode]                  AS  VARCHAR(200)) AS [EnhancedStatusCode]               
		,CAST([SMTPBounceReason]                    AS  VARCHAR(200)) AS [SMTPBounceReason]                 
		,CAST([SMTPMessage]                         AS  VARCHAR(200)) AS [SMTPMessage]                      
		,CAST([DefaultVal]                          AS  VARCHAR(200)) AS [DefaultVal]                       
		,CAST(convert(datetime, [md_record_ingestion_timestamp],103) as datetime)     AS [md_record_ingestion_timestamp] 
		,CAST([md_record_ingestion_pipeline_id]     AS  VARCHAR(500)) AS [md_record_ingestion_pipeline_id]  
		,CAST([md_source_system]                    AS  VARCHAR(200)) AS [md_source_system]                 
		,getdate() as [md_record_written_timestamp]
		,@pipelineid [md_record_written_pipeline_id] 
		,@jobid [md_transformation_job_id] 
	    FROM [stage].[sfmc_bounce]
	
		
		OPTION (LABEL = 'AADPSTDSFMCBNC');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCBNC'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		
		truncate table [stage].[sfmc_bounce];
		
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_bounce;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.sfmc_bounce WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_bounce' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO


