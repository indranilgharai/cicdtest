/****** Object:  StoredProcedure [cons_customer].[sp_ga_hits]    Script Date: 5/19/2022 10:25:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create PROC [cons_customer].[sp_ga_hits] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
	/* Reset value is passed when the Job is triggered
	*/
		IF @reset = 0
		BEGIN

/*DELETE FROM TARGET IF DATA FOR THE DATES AVAILABLE IN STG IS ALREADY PRESENT, 
AND RELOAD DATES [AVAILABLE IN STG] FROM STD TO CONS*/

delete from cons_customer.ga_hits
where date in (select distinct date from [stage].[bq_ga_sessions_raw])


insert into cons_customer.ga_hits
select distinct  
concat(visitstarttime,fullvisitorid,visitid) concat_key,
concat(visitstarttime,fullvisitorid,visitid,pagepath) concat_pagepath_key,
cast([date] as date) [date],
[visitStartTime],
[fullVisitorId],
[VisitID],
cast(hitnumber as int) hitnumber,
[pageviews],
[timeOnSite],
[bounces],
[transactions],
[screenviews],
[uniqueScreenviews],
[timeOnScreen],
[referralPath],
[keyword],
[adContent],
[browser],
[deviceCategory],
[country],
[region],
[type],
cast([time_on_page] as tinyint) [time_on_page],
cast([isEntrance] as tinyint) [isEntrance],
cast([isExit] as tinyint) [isExit],
[pagePath],
[searchKeyword],
[searchCategory],
[pagePathLevel1],
[pagePathLevel4],
[transactionId],
[eventCategory],
[eventAction],
[eventLabel],
[action_type],
[step],
[channelGrouping],
[contentGroup1],
[contentGroup2],
[cd_1],
[cd_2],
[cd_4],
[cd_5],
[cd_7],
[cd_13],
[cd_14],
[cd_16],
[cd_17],
[cd_18],
[cd_20],
[cd_22],
[cd_23],
[cd_24],
[cd_25],
[cd_26],
[cd_27],
[cd_28],
[cd_29],
[cd_31],
[cd_32],
[cd_33],
[md_record_ingestion_timestamp],
getdate() md_record_written_timestamp
from cons_customer.ga_hits_wide WITH (NOLOCK) 
where "date" in (select distinct "date" from [stage].[bq_ga_sessions_raw])

	OPTION (LABEL = 'AADCONSGAHITS');
					   
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label = 'AADCONSGAHITS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			
		END
		ELSE
			--##uncomment it after logic for loading column : md_record_written_timestamp is finalised##
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_customer.ga_hits;
			SELECT @onlydate = CAST(@newrec AS DATE);
			--PRINT @onlydate
			DELETE FROM cons_customer.ga_hits WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.sp_ga_hits' AS ErrorProcedure
			,-- here the sp name u give should be exact same value u give for this sp in sp_name column of meta table: [meta_ctl].[transformation_job_steps]
			ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END