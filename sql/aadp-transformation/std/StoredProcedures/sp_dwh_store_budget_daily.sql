/****** Object:  StoredProcedure [std].[sp_dwh_store_budget_daily]    Script Date: 12/4/2022 10:44:35 AM ******/
SET
	ANSI_NULLS ON
GO
SET
	QUOTED_IDENTIFIER ON
GO
	CREATE PROC [std].[sp_dwh_store_budget_daily] @jobid [int],
	@step_number [int],
	@reset [bit],
	@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN IF EXISTS (
		SELECT
			TOP 1 *
		FROM
			stage.dwh_store_budget_daily
	) BEGIN 
	
	TRUNCATE TABLE std.dwh_store_budget_daily;

PRINT 'INSIDE LOOP'
insert into
	[std].[dwh_store_budget_daily] (
		[sbs_no],
		[store_no],
		[budget_yyyymmdd],
		[budget_year],
		[budget_fy],
		[budget_month],
		[budget_day],
		[budget_dow],
		[budget_week_no],
		[sales_budget],
		[md_record_ingestion_timestamp],
		[md_record_ingestion_pipeline_id],
		[md_source_system],
		[md_record_written_timestamp],
		[md_record_written_pipeline_id],
		[md_transformation_job_id]
	)
select
	[sbs_no] as sbs_no,
	[store_no] as store_no,
	cast([budget_yyyymmdd] as varchar(20)) as budget_yyyymmdd,
	[budget_year] as budget_year,
	[budget_fy] as budget_fy,
	cast([budget_month] as int) as budget_month,
	cast([budget_day] as int) as budget_day,
	[budget_dow] as budget_dow,
	cast([budget_week_no] as int) as budget_week_no,
	cast([sales_budget] as float) as sales_budget,
	CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
	[md_record_ingestion_pipeline_id] as md_record_ingestion_pipeline_id,
	[md_source_system] as md_source_system,
	getdate() AS md_record_written_timestamp,
	@pipelineid md_record_written_pipeline_id,
	@jobid md_transformation_job_id
from
	stage.dwh_store_budget_daily OPTION (LABEL = 'AADPSTDDWHBIDDLY');

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
	@label = 'AADPSTDDWHBIDDLY' EXEC meta_ctl.sp_row_count @jobid,
	@step_number,
	@label 
	
TRUNCATE TABLE stage.dwh_store_budget_daily;

PRINT 'TRUNCATED STAGE'
END
ELSE BEGIN PRINT 'Stage is Empty'
END
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
	@newrec = max(md_record_written_timestamp)
FROM
	std.dwh_store_budget_daily;

SELECT
	@onlydate = CAST(@newrec AS DATE);

DELETE FROM
	std.dwh_store_budget_daily
WHERE
	md_record_written_timestamp = @newrec;

END
END TRY BEGIN CATCH --ERROR OCCURED
PRINT 'ERROR SECTION INSERT'
INSERT
	meta_audit.transform_error_log_sp
SELECT
	ERROR_NUMBER() AS ErrorNumber,
	ERROR_SEVERITY() AS ErrorSeverity,
	ERROR_STATE() AS ErrorState,
	'std.sp_dwh_store_budget_daily' AS ErrorProcedure,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() AS Updated_date
END CATCH
END