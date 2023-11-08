/****** Object:  StoredProcedure [std].[sp_kepler_incoming]    Script Date: 1/25/2023 10:35:06 AM ******/
/*** Modified Stored Procedure [Rank is replaced with Row_number in delete staement] Modified Date 05/05/2023 ***/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_kepler_incoming] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN
INSERT INTO
	[std].[kepler_incoming] (
		incoming_key,
		[sbs_no],
		[store_no],
		[traffic_date],
		[traffic_hour],
		[store_operating],
		[outside_traffic_outside_work_hours],
		[outside_traffic_during_work_hours],
		[inside],
		[shopfront_conversion],
		[dwell_time_total_seconds],
		[br60_qty],
		[br120_qty],
		[md_record_ingestion_timestamp],
		[md_record_ingestion_pipeline_id],
		[md_source_system],
		[md_record_written_timestamp],
		[md_record_written_pipeline_id],
		[md_transformation_job_id]
	)
SELECT
	cast(
		concat(FORMAT(CAST(sbs_no AS INT), '00', 'en-US') , FORMAT(CAST(store_no AS INT), '000', 'en-US'), format(cast(traffic_date as date), 'yyyyMMdd'), traffic_hour) as varchar(50)
	) as incoming_key,
	cast([sbs_no] as int) [sbs_no],
	cast([store_no] as int) [store_no],
	cast([traffic_date] as date) [traffic_date],
	cast([traffic_hour] as int) [traffic_hour],
	cast([store_operating] as int) [store_operating],
	cast([outside_traffic_outside_work_hours] as int) [outside_traffic_outside_work_hours],
	cast([outside_traffic_during_work_hours] as int) [outside_traffic_during_work_hours],
	cast([inside] as int) [inside],
	cast([shopfront_conversion] as float) [shopfront_conversion],
	cast([dwell_time_total_seconds] as bigint) [dwell_time_total_seconds],
	cast([br60_qty] as float) [br60_qty],
	cast([br120_qty] as float) [br120_qty],
	CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
	cast(
		[md_record_ingestion_pipeline_id] as varchar(500)
	) md_record_ingestion_pipeline_id,
	cast([md_source_system] as varchar(100)) md_source_system,
	getdate() AS [md_record_written_timestamp],
	@pipelineid [md_record_written_pipeline_id],
	@jobid [md_transformation_job_id]
FROM
	(select * from [stage].[kepler_incoming]
	where isdate(traffic_date)=1
	and isnumeric(sbs_no)=1 and isnumeric(store_no)=1
	and isnumeric(store_operating)=1
	and isnumeric(traffic_hour)=1 )sub
	OPTION (LABEL = 'AADPSTDKPLRINCG');

WITH incoming AS (
	SELECT
		*,
		row_number() OVER (/* replaced rank() function with row_number() to avoid duplication issue*/
			PARTITION BY [sbs_no],
		[store_no],
		[traffic_date],
		[traffic_hour]
			ORDER BY
				[md_record_ingestion_timestamp] DESC,
				md_record_written_timestamp DESC
		) AS dupcnt
	FROM
		std.kepler_incoming
)
DELETE FROM
	incoming
WHERE
	dupcnt > 1;

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
	@label = 'AADPSTDKPLRINCG' EXEC meta_ctl.sp_row_count @jobid,
	@step_number,
	@label 
	
truncate table [stage].[kepler_incoming];

END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
	@newrec = max(md_record_written_timestamp)
FROM
	std.kepler_incoming;

SELECT
	@onlydate = CAST(@newrec AS DATE);

DELETE FROM
	std.kepler_incoming
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
	'std.sp_kepler_incoming' AS ErrorProcedure,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() AS Updated_date
END CATCH
END