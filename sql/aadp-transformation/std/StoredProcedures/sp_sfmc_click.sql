/****** Object:  StoredProcedure [std].[sp_sfmc_click]    Script Date: 4/12/2022 5:40:57 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create PROC [std].[sp_sfmc_click] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
		insert into [std].[sfmc_click]
		SELECT DISTINCT
	      CAST([HashKey]                           as varchar(200)) as [HashKey]                        
       ,CAST([AccountID]                         as BIGINT ) as [AccountID]                      
       ,CAST([OYBAccountID]                      as BIGINT ) as [OYBAccountID]                   
       ,CAST([JobID]                             as BIGINT ) as [JobID]                          
       ,CAST([ListID]                            as BIGINT ) as [ListID]                         
       ,CAST([BatchID]                           as BIGINT ) as [BatchID]                        
       ,CAST([SubscriberID]                      as BIGINT ) as [SubscriberID]                   
       ,CAST([SubscriberKey]                     as varchar(200)) as [SubscriberKey]                  
       ,CAST([EventDate]                         as DATETIME) as [EventDate]                      
       ,CAST([Domain]                            as varchar(200)) as [Domain]                         
       ,CAST([URL]                               as NVARCHAR(2000)) as [URL]                            
       ,CAST([LinkName]                          as NVARCHAR(2000)) as [LinkName]                       
       ,CAST([LinkContent]                       as NVARCHAR(2000)) as [LinkContent]                    
       ,CAST([IsUnique]                          as VARCHAR(200)) as [IsUnique]                       
       ,CAST([TriggererSendDefinitionObjectID]   as VARCHAR(200)) as [TriggererSendDefinitionObjectID]
       ,CAST([TriggeredSendCustomerKey]          as VARCHAR(200)) as [TriggeredSendCustomerKey]       
       ,CAST([EventDateText]                     as DATETIME) as [EventDateText]                  
       ,CAST( convert(datetime,[md_record_ingestion_timestamp],103) as datetime) as [md_record_ingestion_timestamp]  
       ,CAST([md_record_ingestion_pipeline_id]   as VARCHAR(500)) as [md_record_ingestion_pipeline_id]
       ,CAST([md_source_system]                  as VARCHAR(200)) as [md_source_system]    
	     ,getdate() as [md_record_written_timestamp]
	     , @pipelineid [md_record_written_pipeline_id] 
	     , @jobid [md_transformation_job_id] 
       FROM [stage].[sfmc_click]

			OPTION (LABEL = 'AADPSTDSFMCCLK');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCCLK'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		
		truncate table [stage].[sfmc_click];
		
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_click;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.sfmc_click WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_click' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END

