/****** Object:  StoredProcedure [std].[sp_sfmc_sent]    Script Date: 4/12/2022 7:28:08 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_sfmc_sent] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN


		insert into [std].[sfmc_sent]
		SELECT DISTINCT CAST([HashKey]     					   						as varchar(200) )             as [HashKey]     					   				
			  ,CAST([AccountID]                        						as  bigint            )          as [AccountID]                        				
			  ,CAST([OYBAccountID]                     						as  bigint            )          as [OYBAccountID]                     				
			  ,CAST([JobID]                            						as  bigint            )          as [JobID]                            				
			  ,CAST([ListID]                           						as  bigint            )          as [ListID]                           				
			  ,CAST([BatchID]                          						as  bigint            )          as [BatchID]                          				
			  ,CAST([SubscriberID]                     						as  bigint            )          as [SubscriberID]                     				
			  ,CAST([SubscriberKey]                    						as  varchar(200)            ) as [SubscriberKey]                    				
			  ,CAST([EventDate]                        						as  datetime            )     as [EventDate]                        				
			  ,CAST([Domain]                           						as  varchar(200)            )  as [Domain]                           				
			  ,CAST([TriggererSendDefinitionObjectID]  						as  varchar(200)            ) as [TriggererSendDefinitionObjectID]  				
			  ,CAST([TriggeredSendCustomerKey]         						as  varchar(200)            )          as [TriggeredSendCustomerKey]         				
			  ,CAST(convert(datetime,[md_record_ingestion_timestamp],103)    as   datetime           ) as md_record_ingestion_timestamp
			  ,CAST([md_record_ingestion_pipeline_id]  						as   varchar(500)           ) as [md_record_ingestion_pipeline_id] 
			  ,CAST([md_source_system]                 						as   varchar(200)           )  as [md_source_system]                
			  ,getdate() as [md_record_written_timestamp]
			  , @pipelineid [md_record_written_pipeline_id] 
			  , @jobid [md_transformation_job_id] 
		  FROM stage.[sfmc_sent]
		  OPTION (LABEL = 'AADPSTDSFMCSENT');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCSENT'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

		truncate table stage.[sfmc_sent];
		
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_sent;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.sfmc_sent WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_sent' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END


