/****** Object:  StoredProcedure [std].[sp_sfmc_person]    Script Date: 4/12/2022 7:21:21 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_sfmc_person] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			truncate table [std].[sfmc_person];

			insert into [std].[sfmc_person]
			SELECT DISTINCT CAST([person_uuid]    							AS VARCHAR(200)  ) as [person_uuid]    							
			  ,CAST([created]        							AS DATETIME     ) as [created]        							
			  ,CAST([locale]         							AS VARCHAR(200)  ) as [locale]         							
			  ,CAST([title]				                        AS VARCHAR(200)  ) as [title]				                        
			  ,CAST([skin_type]                                 AS VARCHAR(200)  ) as [skin_type]                                 
			  ,CAST([customer_group_id]                         AS VARCHAR(200)  ) as [customer_group_id]                         
			  ,CAST([last_surveyed]                             AS datetime     ) as [last_surveyed]                             
			  ,CAST([globalLanguage]                            AS varchar(200)  ) as [globalLanguage]                            
			  ,CAST([optinEmail]                                AS varchar(200)  ) as [optinEmail]                                
			  ,CAST([optinEmailLastUpdatedDate]                 AS datetime     ) as [optinEmailLastUpdatedDate]                 
			  ,CAST([optinMobile]                               AS varchar(200)  ) as [optinMobile]                               
			  ,CAST([optinMobileLastUpdatedDate]                AS datetime     ) as [optinMobileLastUpdatedDate]                
			  ,CAST([pushOptin]                                 AS VARCHAR(200)  ) as [pushOptin]                                 
			  ,CAST([pushOptinLastUpdatedDate]                  AS DATEtime     ) as [pushOptinLastUpdatedDate]                  
			  ,CAST([directMailOptin]                           AS varchar(200)  ) as [directMailOptin]                           
			  ,CAST([directMailOptinLastUpdatedDate]            AS datetime     ) as [directMailOptinLastUpdatedDate]            
			  ,CAST([telephoneMarketingOptin]                   AS VARCHAR(200)  ) as [telephoneMarketingOptin]                   
			  ,CAST([telephoneMarketingOptinLastUpdatedDate]    AS DATEtime     ) as [telephoneMarketingOptinLastUpdatedDate]    
			  ,CAST([messagingAppOptin]                         AS VARCHAR(200)  ) as [messagingAppOptin]                         
			  ,CAST([messagingAppOptinLastUpdatedDate]          AS datetime     ) as [messagingAppOptinLastUpdatedDate]          
			  ,CAST([emailBounceDate]                           AS datetime     ) as [emailBounceDate]                           
			  ,CAST([mobileBounceDate]                          AS datetime     ) as [mobileBounceDate]                          
			  ,CAST([consentToThirdPartyMarketing]              AS varchar(200)      ) as [consentToThirdPartyMarketing]              
			  ,CAST([consentToShareDataAbroad]                  AS varchar(200)     ) as [consentToShareDataAbroad]                  
			  ,CAST([onlineAccountFlag]                         AS varchar(200)  ) as [onlineAccountFlag]                         
			  ,CAST([onlineAccountcreateDate]                   AS datetime     ) as [onlineAccountcreateDate]                   
			  ,CAST([SubscriptionType]                          AS varchar(200)  ) as [SubscriptionType]                          
			  ,CAST([ml_updated_date]                           AS datetime     ) as [ml_updated_date]                           
			  ,CAST([sfmc_inserted_cst_date]                    AS datetime     ) as [sfmc_inserted_cst_date]                    
			  ,CAST([source]                                    AS varchar(200)  ) as [source]                                    
			  ,CAST([sfmc_updated_cst_date]                     AS datetime     ) as [sfmc_updated_cst_date]                     
			  ,CAST([accept_all_terms]                          AS varchar(200)  ) as [accept_all_terms]                          
			  ,CAST([EmailUnsubReason]                          AS varchar(200) ) as [EmailUnsubReason]                          
			  ,CAST([ConsentToThirdPartyMarketingDate]          AS datetime     ) as [ConsentToThirdPartyMarketingDate]          
			  ,CAST([ConsentToShareDataAbroadDate]              AS datetime     ) as [ConsentToShareDataAbroadDate]              
			  ,CAST([globalCountry]                             AS varchar(200)  ) as [globalCountry]                             
			  ,CAST([Home_store]                                AS varchar(200)  ) as [Home_store]                                
		    ,CAST(convert(datetime,[md_record_ingestion_timestamp],103) AS datetime) as md_record_ingestion_timestamp
			  ,CAST([md_record_ingestion_pipeline_id]           AS varchar(500) ) as [md_record_ingestion_pipeline_id]   
			  ,CAST([md_source_system]                          AS varchar(200)  ) as [md_source_system]                  
			  ,CAST([mobileConnectLocale]                       AS varchar(200) ) as [mobileConnectLocale]               
			  ,CAST([email]                                     AS varchar(200)) as [email]     
			  ,getdate() as [md_record_written_timestamp]
			  , @pipelineid [md_record_written_pipeline_id] 
			  , @jobid [md_transformation_job_id] 
		  FROM stage.[sfmc_person]
		  where CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME)
			in (select max(CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME)) from stage.[sfmc_person] )
 
  				OPTION (LABEL = 'AADPSTDSFMCPRSN');

				  UPDATE STATISTICS [std].[sfmc_person]; 
				  UPDATE STATISTICS stage.sfmc_person;

					--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
					DECLARE @label VARCHAR(500)

					SET @label = 'AADPSTDSFMCPRSN'

					EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_person;

			SELECT @onlydate = CAST(@newrec AS DATE);

			
			DELETE FROM std.sfmc_person WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_person' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END

