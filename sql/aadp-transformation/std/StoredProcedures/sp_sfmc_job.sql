/****** Object:  StoredProcedure [std].[sp_sfmc_job]    Script Date: 4/12/2022 7:00:18 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create PROC [std].[sp_sfmc_job] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
insert into [std].[sfmc_job]
    SELECT DISTINCT 
	   CAST ([HashKey]                             AS VARCHAR(200) ) AS [HashKey]                          
      ,CAST ([JobID]                               AS bigint ) AS [JobID]                            
      ,CAST ([EmailID]                             AS bigint ) AS [EmailID]                          
      ,CAST ([AccountID]                           AS bigint ) AS [AccountID]                        
      ,CAST ([AccountUserID]                       AS bigint) AS [AccountUserID]                    
      ,CAST ([FromName]                            AS VARCHAR(200) ) AS [FromName]                         
      ,CAST ([FromEmail]                           AS VARCHAR(200) ) AS [FromEmail]                        
      ,CAST ([SchedTime]                           AS DATETIME ) AS [SchedTime]                        
      ,CAST ([PickupTime]                          AS DATETIME ) AS [PickupTime]                       
      ,CAST ([DeliveredTime]                       AS DATETIME ) AS [DeliveredTime]                    
      ,CAST ([EventID]                             AS VARCHAR ) AS [EventID]                          
      ,CAST ([IsMultipart]                         AS VARCHAR ) AS [IsMultipart]                      
      ,CAST ([JobType]                             AS VARCHAR(200) ) AS [JobType]                          
      ,CAST ([JobStatus]                           AS VARCHAR(200) ) AS [JobStatus]                        
      ,CAST ([ModifiedBy]                          AS bigint ) AS [ModifiedBy]                       
      ,CAST ([ModifiedDate]                        AS DATETIME ) AS [ModifiedDate]                     
      ,CAST ([EmailName]                           AS VARCHAR(200) ) AS [EmailName]                        
      ,CAST ([EmailSubject]                        AS VARCHAR(200) ) AS [EmailSubject]                     
      ,CAST ([IsWrapped]                           AS VARCHAR(200) ) AS [IsWrapped]                        
      ,CAST ([TestEmailAddr]                       AS VARCHAR(200) ) AS [TestEmailAddr]                    
      ,CAST ([Category]                            AS VARCHAR(200) ) AS [Category]                         
      ,CAST ([BccEmail]                            AS VARCHAR(200) ) AS [BccEmail]                         
      ,CAST ([OriginalSchedTime]                   AS DATETIME ) AS [OriginalSchedTime]                
      ,CAST ([CreatedDate]                         AS DATETIME ) AS [CreatedDate]                      
      ,CAST ([CharacterSet]                        AS VARCHAR(200) ) AS [CharacterSet]                     
      ,CAST ([IPAddress]                           AS VARCHAR(200) ) AS [IPAddress]                        
      ,CAST ([SalesForceTotalSubscriberCount]      AS bigint ) AS [SalesForceTotalSubscriberCount]   
      ,CAST ([SalesForceErrorSubscriberCount]      AS bigint ) AS [SalesForceErrorSubscriberCount]   
      ,CAST ([SendType]                            AS VARCHAR(200) ) AS [SendType]                         
      ,CAST ([DynamicEmailSubject]                 AS VARCHAR(200) ) AS [DynamicEmailSubject]              
      ,CAST ([SuppressTracking]                    AS VARCHAR(200) ) AS [SuppressTracking]                 
      ,CAST ([SendClassificationType]              AS VARCHAR(200) ) AS [SendClassificationType]           
      ,CAST ([SendClassification]                  AS VARCHAR(200) ) AS [SendClassification]               
      ,CAST ([ResolveLinksWithCurrentData]         AS VARCHAR(200) ) AS [ResolveLinksWithCurrentData]      
      ,CAST ([EmailSendDefinition]                 AS VARCHAR(200) ) AS [EmailSendDefinition]              
      ,CAST ([DeduplicateByEmail]                  AS VARCHAR(200) ) AS [DeduplicateByEmail]               
      ,CAST ([TriggererSendDefinitionObjectID]     AS VARCHAR(200) ) AS [TriggererSendDefinitionObjectID]  
      ,CAST ([TriggeredSendCustomerKey]            AS VARCHAR(200) ) AS [TriggeredSendCustomerKey]         
      ,CAST (CONVERT(DATETIME,[md_record_ingestion_timestamp] ,103)  AS DATETIME ) AS [md_record_ingestion_timestamp]    
      ,CAST ([md_record_ingestion_pipeline_id]     AS VARCHAR(500) ) AS [md_record_ingestion_pipeline_id]  
      ,CAST ([md_source_system]                    AS VARCHAR(200) ) AS [md_source_system] 
	    ,getdate() as [md_record_written_timestamp]
	    , @pipelineid [md_record_written_pipeline_id] 
	    , @jobid [md_transformation_job_id] 
      FROM [stage].[sfmc_job]

  		OPTION (LABEL = 'AADPSTDSFMCJB');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPSTDSFMCJB'

			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

      truncate table [stage].[sfmc_job];

		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME
				,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_job;

			SELECT @onlydate = CAST(@newrec AS DATE);

			
			DELETE FROM std.sfmc_job WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_job' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END

