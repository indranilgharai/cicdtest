
/****** Object:  StoredProcedure [cons_customer].[sp_ga_session_wide]    Script Date: 5/20/2022 5:16:42 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create PROC [cons_customer].[sp_ga_session_wide] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
	/* Reset value is passed when the Job is triggered
	*/
		IF @reset = 0
		BEGIN


/*DELETE FROM TARGET IF DATA FOR THE DATES AVAILABLE IN STG IS ALREADY PRESENT, 
AND RELOAD DATES [AVAILABLE IN STG] FROM STD TO CONS*/

delete from cons_customer.ga_session_wide
where "date" in (select distinct "date" from  [stage].[bq_ga_sessions_raw])


insert into cons_customer.ga_session_wide
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
cast(totalTransactionRevenue as float)/1000000	as totalTransactionRevenue,
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
channelGrouping	,
socialEngagementType	,
cd_1	,
cd_2	,
cd_6	,
cd_7	,
cd_8	,
cd_9	,
cd_13	,
cd_14	,
cd_17	,
cd_18	,
cd_20	,
cd_29	,
cd_31	,
cd_32	,
cd_33	,
[md_record_ingestion_timestamp],
[md_record_ingestion_pipeline_id],
[md_source_system],
getdate() [md_record_written_timestamp] ,
@pipelineid md_record_written_pipeline_id,
@jobid md_transformation_job_id 
from [std].[ga_sessions_raw] WITH (NOLOCK) 
where "date" in (select distinct "date" from  [stage].[bq_ga_sessions_raw])

	OPTION (LABEL = 'AADCONSGASESSIONWD');
					   
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label = 'AADCONSGASESSIONWD'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label


		END
		ELSE
			--##uncomment it after logic for loading column : md_record_written_timestamp is finalised##
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_customer.ga_session_wide;
			SELECT @onlydate = CAST(@newrec AS DATE);
			--PRINT @onlydate
			DELETE FROM cons_customer.ga_session_wide WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.sp_ga_session_wide' AS ErrorProcedure
			,-- here the sp name u give should be exact same value u give for this sp in sp_name column of meta table: [meta_ctl].[transformation_job_steps]
			ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END