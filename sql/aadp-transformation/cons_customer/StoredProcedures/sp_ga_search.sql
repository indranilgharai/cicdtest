SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_customer].[sp_ga_search] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
	/* Reset value is passed when the Job is triggered
	*/
		IF @reset = 0
begin



/*DELETE FROM TARGET IF DATA FOR THE DATES AVAILABLE IN STG IS ALREADY PRESENT, 
AND RELOAD DATES [AVAILABLE IN STG] FROM STD TO CONS*/

delete from cons_customer.ga_search
where "date" in (select distinct "date" from [stage].[bq_ga_sessions_raw]);


WITH step_zero as (

SELECT 
cast("date" as date) AS date
,fullVisitorId
,VisitID
,visitStartTime
,clientid
,hitnumber
,time
,type
,count(searchkeyword) over (PARTITION BY fullVisitorId, visitStartTime,date order by hitNumber ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 0 FOLLOWING) as grp
,isinteraction
,isexit
,searchKeyword
,pagepath
,eventCategory
,eventAction
,eventLabel
,[md_record_ingestion_timestamp]
,[md_record_ingestion_pipeline_id]
,[md_source_system]
from [std].[ga_sessions_raw] WITH (NOLOCK) 
where "date" in (select distinct "date" from [stage].[bq_ga_sessions_raw])
),step_one AS (

SELECT 
cast("date" as date) AS date
,fullVisitorId
,VisitID
,visitStartTime
,clientid
,hitnumber
,cast(time as float)/ 1000 as hit_secs
,type
,isinteraction
,isexit
,searchKeyword
,pagepath
,eventCategory
,eventAction
,eventLabel
,grp
,HASHBYTES('MD5', max(searchkeyword) over (partition by fullVisitorId, visitStartTime,date , grp)) hashkey 
,max(searchkeyword) over (partition by fullVisitorId, visitStartTime,date , grp) as searchterm
,LEAD(cast(time as float)/ 1000) OVER (PARTITION BY fullVisitorId, visitStartTime,date ORDER BY hitNumber ASC) - (cast(time as float)/ 1000) AS time_on_hit
,[md_record_ingestion_timestamp]
,[md_record_ingestion_pipeline_id]
,[md_source_system]
from step_zero

)

-- Step two - calculate some of the metrics such as if the search term changes (search refinement) or how long is spent on each term before it changes (time after search)
, step_two AS (
SELECT 
date
,fullVisitorId
,VisitID
,visitStartTime
,clientid
,hitnumber
,hit_secs
,time_on_hit
,type
,isexit
,searchKeyword
,searchterm
,hashkey
-- Search Refinements = The number of times a user searched again immediately after performing a search.
,CASE WHEN LEAD(hashkey) OVER (PARTITION BY fullVisitorId, visitStartTime,date ORDER BY hitNumber ASC) <> hashkey THEN 1 ELSE 0 END AS search_refinement

-- Search Exits = The number of searches made immediately before leaving the site.
,CASE WHEN upper(isexit) in ('TRUE','1') AND searchKeyword IS NOT NULL THEN 1 ELSE 0 END AS search_exit

-- Time after Search = The amount of time users spend on your site after performing a search. This is calculated as Sum of all search_duration across all searches / (search_transitions + 1)
,SUM(time_on_hit) OVER (PARTITION BY fullVisitorId, visitStartTime, searchterm,hashkey,date) AS time_after_search

-- Search Depth = The number of pages viewed after performing a search. This is calculated as Sum of all search_depth across all searches / (search_transitions + 1)
,SUM(CASE WHEN upper(type) = 'PAGE' AND searchKeyword IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY fullVisitorId, visitStartTime, searchterm,hashkey,date) AS search_depth
,[md_record_ingestion_timestamp]
,[md_record_ingestion_pipeline_id]
,[md_source_system]
FROM step_one

)


-- Step three - Aggregate to one row per session per keyword counting how many pageviews per term and if there was an exit etc
insert into cons_customer.ga_search
SELECT 
date
,fullVisitorId
,VisitID
,visitStartTime
,clientid
,searchterm
,hashkey 
-- Results Pageviews / Search = Pageviews of search result pages / Total Unique Searches.
,COUNT(searchKeyword) AS results_pageviews
-- Search Exits = The number of searches made immediately before leaving the site.
,SUM(search_exit) AS search_exits
-- Search Refinements = The number of times a user searched again immediately after performing a search.
,SUM(search_refinement) AS search_refinements
-- Time after Search = The amount of time users spend on your site after performing a search. This is calculated as Sum of all search_duration across all searches / (search_transitions + 1)
,MAX(time_after_search) AS time_after_search
-- Search Depth = The number of pages viewed after performing a search. This is calculated as Sum of all search_depth across all searches / (search_transitions + 1)
,MAX(search_depth) AS search_depth

,[md_record_ingestion_timestamp]
,[md_record_ingestion_pipeline_id]
,[md_source_system]
,getdate() [md_record_written_timestamp] 
,@pipelineid as md_record_written_pipeline_id
,@jobid as  md_transformation_job_id 
FROM step_two
WHERE searchterm IS NOT NULL
and hashkey is not null
GROUP BY 
date
,fullVisitorId
,VisitID
,visitStartTime
,clientid
,searchterm
,hashkey
,[md_record_ingestion_timestamp]
,[md_record_ingestion_pipeline_id]
,[md_source_system]



	OPTION (LABEL = 'AADCONSGASERCH');
					   
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label = 'AADCONSGASERCH'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

		

		END
		ELSE
			--##uncomment it after logic for loading column : md_record_written_timestamp is finalised##
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_customer.ga_search;
			SELECT @onlydate = CAST(@newrec AS DATE);
			--PRINT @onlydate
			DELETE FROM cons_customer.ga_search WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.sp_ga_search' AS ErrorProcedure
			,-- here the sp name u give should be exact same value u give for this sp in sp_name column of meta table: [meta_ctl].[transformation_job_steps]
			ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END