/****** Object:  StoredProcedure [std].[sp_dwh_store_budget_monthly]    Script Date: 12/4/2022 10:45:42 AM ******/
SET
	ANSI_NULLS ON
GO
SET
	QUOTED_IDENTIFIER ON
GO
	CREATE PROC [std].[sp_dwh_store_budget_monthly] @jobid [int],
	@step_number [int],
	@reset [bit],
	@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN IF EXISTS (
		SELECT
			TOP 1 *
		FROM
			stage.dwh_store_budget_monthly
	) BEGIN TRUNCATE TABLE std.dwh_store_budget_monthly;

PRINT 'INSIDE LOOP'
insert into
	[std].[dwh_store_budget_monthly] (
		[sbs_no],
		[store_no],
		[budget_year],
		[budget_fy],
		[budget_month],
		[sales_budget],
		[mon_contribution],
		[tue_contribution],
		[wed_contribution],
		[thu_contribution],
		[fri_contribution],
		[sat_contribution],
		[sun_contribution],
		[rent_amount],
		[md_record_ingestion_timestamp],
		[md_record_ingestion_pipeline_id],
		[md_source_system],
		[md_record_written_timestamp],
		[md_record_written_pipeline_id],
		[md_transformation_job_id]
	)
select
	cast([sbs_no] as [int]) sbs_no,
	cast([store_no] as [int]) [store_no],
	cast([budget_year] as [int]) [budget_year],
	cast([budget_fy] as [int]) [budget_fy],
	cast([budget_month] as [int]) [budget_month],
	cast([sales_budget] as float) as [sales_budget],
	cast([mon_contribution] as [float]) [mon_contribution],
	cast([tue_contribution] as [float]) [tue_contribution],
	cast([wed_contribution] as [float]) [wed_contribution],
	cast([thu_contribution] as [float]) [thu_contribution],
	cast([fri_contribution] as [float]) [fri_contribution],
	cast([sat_contribution] as [float]) [sat_contribution],
	cast([sun_contribution] as [float]) [sun_contribution],
	cast([rent_amount] as float) as [rent_amount],
	CAST(
		CONVERT(DATETIME, [md_record_ingestion_timestamp], 103) AS DATETIME
	) AS [md_record_ingestion_timestamp],
	CAST(
		[md_record_ingestion_pipeline_id] AS VARCHAR(500)
	) AS [md_record_ingestion_pipeline_id],
	CAST([md_source_system] AS VARCHAR(100)) AS [md_source_system],
	getdate() AS [md_record_written_timestamp],
	@pipelineid [md_record_written_pipeline_id],
	@jobid [md_transformation_job_id]
from
	stage.dwh_store_budget_monthly OPTION (LABEL = 'AADPSTDDWHBIDMON');

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
	@label = 'AADPSTDDWHBIDMON' EXEC meta_ctl.sp_row_count @jobid,
	@step_number,
	@label 
	
TRUNCATE TABLE stage.dwh_store_budget_monthly;

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
	std.dwh_store_budget_monthly;

SELECT
	@onlydate = CAST(@newrec AS DATE);

DELETE FROM
	std.dwh_store_budget_monthly
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
	'std.sp_dwh_store_budget_monthly' AS ErrorProcedure,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() AS Updated_date
END CATCH
END