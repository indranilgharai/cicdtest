/****** Object:  StoredProcedure [std].[sp_dwh_store_sales_target_daily]    Script Date: 12/4/2022 10:47:14 AM ******/
SET
	ANSI_NULLS ON
GO
SET
	QUOTED_IDENTIFIER ON
GO
	CREATE PROC [std].[sp_dwh_store_sales_target_daily] @jobid [int],
	@step_number [int],
	@reset [bit],
	@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN IF EXISTS (
		SELECT
			TOP 1 *
		FROM
			stage.dwh_store_sales_target_daily
	) BEGIN truncate table [std].[dwh_store_sales_target_daily];

PRINT 'INSIDE LOOP'
insert into
	[std].[dwh_store_sales_target_daily] (
		sbs_no,
		store_no,
		store_name,
		yyyymmdd,
		daily_target,
		available,
		userid,
		date_unix,
		year,
		md_record_ingestion_timestamp,
		md_record_ingestion_pipeline_id,
		md_source_system,
		[md_record_written_timestamp],
		[md_record_written_pipeline_id],
		[md_transformation_job_id]
	)
select
	cast(sbs_no as int) sbs_no,
	cast(store_no as int) store_no,
	cast(store_name as varchar(300)) store_name,
	cast(yyyymmdd as varchar(20)) yyyymmdd,
	cast(daily_target as float) daily_target,
	cast(available as int) available,
	cast(userid as varchar(200)) userid,
	cast(date_unix as varchar(200)) date_unix,
	cast(year as int) year,
	CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
	cast(md_record_ingestion_pipeline_id as varchar(500)) md_record_ingestion_pipeline_id,
	cast(md_source_system as varchar(50)) md_source_system,
	getdate() AS md_record_written_timestamp,
	@pipelineid md_record_written_pipeline_id,
	@jobid md_transformation_job_id
from
	stage.dwh_store_sales_target_daily where len(yyyymmdd) = 8
	-- added filter to get correct yyyymmdd values
	OPTION (LABEL = 'AADPSTDDWHSALDLY');

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
	@label = 'AADPSTDDWHSALDLY' EXEC meta_ctl.sp_row_count @jobid,
	@step_number,
	@label 
	
TRUNCATE TABLE stage.dwh_store_sales_target_daily;

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
	std.dwh_store_sales_target_daily;

SELECT
	@onlydate = CAST(@newrec AS DATE);

DELETE FROM
	std.dwh_store_sales_target_daily
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
	'std.sp_dwh_store_sales_target_daily' AS ErrorProcedure,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() AS Updated_date
END CATCH
END