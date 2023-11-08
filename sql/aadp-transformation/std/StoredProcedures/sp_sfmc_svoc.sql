/****** Object:  StoredProcedure [std].[sp_sfmc_svoc]    Script Date: 4/12/2022 7:47:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_sfmc_svoc] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN

			truncate table [std].[sfmc_svoc];

			insert into [std].[sfmc_svoc]
			select DISTINCT cast(contactkey as varchar(200)) contactkey
			,cast(email as varchar(200)) as email
			,cast(mobileConnectLocale as varchar(200)) as mobileConnectLocale
			,cast(locale as varchar(200)) as locale
			,cast(globalCountry as varchar(200)) as globalCountry
			,cast(globalLanguage as varchar(200)) as globalLanguage
			,cast(title as varchar(200))  as title
			,cast(skin_type as varchar(200)) as skin_type
			,cast(customer_group_id as varchar(200)) as customer_group_id
			,cast(last_surveyed as datetime) as last_surveyed
			,cast(optinEmail as varchar(200)) as optinEmail
			,cast(optinEmailLastUpdatedDate as datetime) as optinEmailLastUpdatedDate
			,cast(emailBounceDate as datetime) as emailBounceDate
			,cast(EmailUnsubReason as varchar(200)) as EmailUnsubReason
			,cast(optinMobile as varchar(200)) as optinMobile
			,cast(optinMobileLastUpdatedDate as datetime) as optinMobileLastUpdatedDate
			,cast(mobileBounceDate as datetime) as mobileBounceDate
			,cast(pushOptin as varchar(200)) as pushOptin
			,cast(pushOptinLastUpdatedDate as datetime) as pushOptinLastUpdatedDate
			,cast(directMailOptin as varchar(200)) as directMailOptin
			,cast(directMailOptinLastUpdatedDate as datetime) as directMailOptinLastUpdatedDate
			,cast(telephoneMarketingOptin as varchar(200)) as telephoneMarketingOptin
			,cast(telephoneMarketingOptinLastUpdatedDate as datetime) as telephoneMarketingOptinLastUpdatedDate
			,cast(messagingAppOptin as varchar(200)) as messagingAppOptin
			,cast(messagingAppOptinLastUpdatedDate as datetime) as messagingAppOptinLastUpdatedDate
			,cast(consentToThirdPartyMarketing as varchar(200)) as consentToThirdPartyMarketing
			,cast(ConsentToThirdPartyMarketingDate as datetime) as ConsentToThirdPartyMarketingDate
			,cast(consentToShareDataAbroad as varchar(200)) as consentToShareDataAbroad
			,cast(ConsentToShareDataAbroadDate as datetime) as ConsentToShareDataAbroadDate
			,cast(accept_all_terms as varchar(200)) as accept_all_terms
			,cast(created as datetime) as created
			,cast(source as varchar(200)) as source
			,cast(SubscriptionType as varchar(200)) as SubscriptionType
			,cast(onlineAccountFlag as varchar(200)) as onlineAccountFlag
			,cast(onlineAccountcreateDate as datetime) as onlineAccountcreateDate
			,cast(first_purchase_channel as varchar(200)) as first_purchase_channel
			,cast(first_purchase_store as varchar(200)) as first_purchase_store
			,cast(first_purchase_subsidiary as varchar(200)) as first_purchase_subsidiary
			,cast(first_purchase_date as datetime) as first_purchase_date
			,cast(last_purchase_channel as varchar(200)) as last_purchase_channel
			,cast(last_purchase_store as varchar(200)) as last_purchase_store
			,cast(last_purchase_subsidiary as varchar(200)) as last_purchase_subsidiary
			,cast(last_purchase_date as datetime) as last_purchase_date
			,cast(lifetime_transactions as bigint) as lifetime_transactions
			,cast(total_revenue_aud as float) as total_revenue_aud
			,cast(most_purchased_subsidiary as varchar(200)) as most_purchased_subsidiary
			,cast(second_purchase_date as datetime) as second_purchase_date
			,cast(random as varchar(200)) as random
			,cast(RFV_Class as varchar(200)) as RFV_Class
			,cast(RFV_Segment_Name as varchar(200)) as RFV_Segment_Name
			,cast(EinsteinEmailEngagementPersona as varchar(200)) as EinsteinEmailEngagementPersona
			,cast(InterestArray as varchar(200)) as InterestArray
			,cast(Sent_180day as bigint) as Sent_180day
			,cast(Unique_Opens_180day as bigint) as Unique_Opens_180day
			,cast(Unique_Clicks_180day as bigint) as Unique_Clicks_180day
			,cast(Sent_90day as bigint) as Sent_90day
			,cast(Unique_Opens_90day as bigint) as Unique_Opens_90day
			,cast(Unique_Clicks_90day as bigint) as Unique_Clicks_90day
			,cast(Sent_30day as bigint) as  Sent_30day
			,cast(Unique_Opens_30day as bigint) as Unique_Opens_30day
			,cast(Unique_Clicks_30day as bigint) as Unique_Clicks_30day
			,cast(sent_lifetime_count as bigint) as sent_lifetime_count
			,cast(open_lifetime_count as bigint)  as open_lifetime_count
			,cast(click_lifetime_count as bigint) as click_lifetime_count
			,cast(Last_Email_Send_Date as datetime) as Last_Email_Send_Date
			,cast(Last_Email_Open_Date as datetime) as Last_Email_Open_Date
			,cast(Last_Email_Click_Date as datetime) as Last_Email_Click_Date
			,cast(Bronto_Open_Rate as float) as Bronto_Open_Rate
			,cast(Bronto_Click_Rate as float) as Bronto_Click_Rate
			,cast(Bronto_Last_Open_Date as datetime) as Bronto_Last_Open_Date
			,cast(Bronto_Last_Click_Date as datetime) as Bronto_Last_Click_Date
			,cast(Bronto_Status as varchar(200)) as Bronto_Status
			,cast(first_txn_store_location_code as varchar(200)) as first_txn_store_location_code
			,cast(first_txn_store_name as varchar(200)) as first_txn_store_name
			,CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp 
			,[md_record_ingestion_pipeline_id] 
			,[md_source_system]		
			,getdate() [md_record_written_timestamp] 
			,@pipelineid as [md_record_written_pipeline_id] 
			,@jobid as [md_transformation_job_id] 	

			from stage.sfmc_svoc
			where CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME)
			in (select max(CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME)) from stage.[sfmc_svoc] )
			OPTION (LABEL = 'AADPSTDSFMCSVOC');
			
			UPDATE STATISTICS [std].[sfmc_svoc];
			UPDATE STATISTICS stage.sfmc_svoc;

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCSVOC'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.sfmc_svoc;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.sfmc_svoc WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_sfmc_svoc' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
