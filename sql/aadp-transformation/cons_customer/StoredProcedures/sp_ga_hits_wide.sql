
/****** Object:  StoredProcedure [cons_customer].[sp_ga_hits_wide]    Script Date: 5/20/2022 5:13:46 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_customer].[sp_ga_hits_wide] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
	/* Reset value is passed when the Job is triggered
	*/
		IF @reset = 0
		BEGIN

/*DELETE FROM TARGET IF DATA FOR THE DATES AVAILABLE IN STG IS ALREADY PRESENT, 
AND RELOAD DATES [AVAILABLE IN STG] FROM STD TO CONS*/

delete from cons_customer.ga_hits_wide
where date in (select distinct date from [stage].[bq_ga_sessions_raw])


insert into cons_customer.ga_hits_wide
select distinct
visitNumber	,
visitId	,
visitStartTime	,
fullVisitorId	,
userId	,
clientId	,
"date"	,
visits	,
hits	,
pageviews	,
timeOnSite	,
bounces	,
transactions	,
newVisits	,
screenviews	,
uniqueScreenviews	,
timeOnScreen	,
referralPath	,
campaign	,
"source"	,
"medium"	,
keyword	,
adContent	,
isTrueDirect	,
campaignCode	,
browser	,
operatingSystem	,
"language"	,
screenResolution	,
deviceCategory	,
continent	,
country	,
region	,
city	,
cityId	,
hitNumber	,
"time"	,
"type"	,
CASE WHEN upper("type") = 'PAGE' THEN 1 ELSE NULL END AS time_on_page,
"hour"	,
"minute"	,
isInteraction	,
isEntrance	,
isExit	,
referer	,
pagePath	,
hostname	,
pageTitle	,
searchKeyword	,
searchCategory	,
pagePathLevel1	,
pagePathLevel2	,
pagePathLevel3	,
pagePathLevel4	,
transactionId	,
cast(hits_transactionRevenue as float)/1000000	as hits_transactionRevenue,
transactionTax	,
transactionShipping	,
affiliation	,
currencyCode	,
cast(localTransactionRevenue as float)/1000000	as localTransactionRevenue,
localTransactionTax	,
localTransactionShipping	,
transactionCoupon	,
eventCategory	,
eventAction	,
eventLabel	,
action_type	,
step	,
"option"	,
socialInteractionNetwork	,
socialInteractionAction	,
socialInteractions	,
socialInteractionTarget	,
socialNetwork	,
uniqueSocialInteractions	,
hasSocialSourceReferral	,
socialInteractionNetworkAction	,
pageLoadSample	,
pageLoadTime	,
dataSource	,
channelGrouping	,
socialEngagementType	,
contentGroup1,
contentGroup2,
cd_1	,
cd_2	,
cd_3	,
cd_4	,
cd_5	,
cd_6	,
cd_7	,
cd_8	,
cd_9	,
cd_10	,
cd_11	,
cd_12	,
cd_13	,
cd_14	,
cd_15	,
cd_16	,
cd_17	,
cd_18	,
cd_19	,
cd_20	,
cd_21	,
cd_22	,
cd_23	,
cd_24	,
cd_25	,
cd_26	,
cd_27	,
cd_28	,
cd_29	,
cd_30	,
cd_31	,
cd_32	,
cd_33	,
[md_record_ingestion_timestamp],
[md_record_ingestion_pipeline_id],
[md_source_system],
getdate() [md_record_written_timestamp] ,
@pipelineid as md_record_written_pipeline_id,
@jobid as  md_transformation_job_id 
from [std].[ga_sessions_raw] WITH (NOLOCK) 
where "date" in (select distinct "date" from [stage].[bq_ga_sessions_raw])

	OPTION (LABEL = 'AADCONSGAHITSWD');
					   
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label = 'AADCONSGAHITSWD'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			
		END
		ELSE
			--##uncomment it after logic for loading column : md_record_written_timestamp is finalised##
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_customer.ga_hits_wide;
			SELECT @onlydate = CAST(@newrec AS DATE);
			--PRINT @onlydate
			DELETE FROM cons_customer.ga_hits_wide WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.sp_ga_hits_wide' AS ErrorProcedure
			,-- here the sp name u give should be exact same value u give for this sp in sp_name column of meta table: [meta_ctl].[transformation_job_steps]
			ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END