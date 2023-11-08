/****** Object:  StoredProcedure [std].[sp_workday_employee]    Script Date: 12/4/2022 10:56:06 AM ******/
SET
	ANSI_NULLS ON
GO
SET
	QUOTED_IDENTIFIER ON
GO
	CREATE PROC [std].[sp_workday_employee] @jobid [int],
	@step_number [int],
	@reset [bit],
	@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN
INSERT INTO
	[std].[workday_employeedata] (
		[employeeCode],
		[employeeEmail],
		[employeeLastName],
		[employeeFirstName],
		[employeeStore],
		[terminationDate],
		[startDate],
		[employeeCountry],
		[employeeJobTitle],
		[employeeStatus],
		[employeeContractHours],
		[employeeStoreName],
		[md_record_ingestion_timestamp],
		[md_record_ingestion_pipeline_id],
		[md_source_system],
		[md_record_written_timestamp],
		[md_record_written_pipeline_id],
		[md_transformation_job_id]
	)
SELECT  DISTINCT
	cast([employeeCode] as varchar(100)) as employeeCode,
	cast([employeeEmail] as varchar(500)) as employeeEmail,
	cast([employeeLastName] as varchar(100)) as employeeLastName,
	cast([employeeFirstName] as varchar(100)) as employeeFirstName,
	cast([employeeStore] as varchar(100)) as employeeStore,
	cast([terminationDate] as varchar(100)) as terminationDate,
	cast([startDate] as varchar(100)) as startDate,
	cast([employeeCountry] as varchar(50)) as employeeCountry,
	cast([employeeJobTitle] as varchar(500)) as employeeJobTitle,
	cast([employeeStatus] as varchar(200)) as employeeStatus,
	cast([employeeContractHours] as float) as employeeContractHours,
	cast([employeeStoreName] as varchar(500)) as employeeStoreName,
	CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME)  as md_record_ingestion_timestamp,
	cast(
		[md_record_ingestion_pipeline_id] as varchar(500)
	) as md_record_ingestion_pipeline_id,
	cast([md_source_system] as varchar(100)) as md_source_system,
	getdate() AS [md_record_written_timestamp],
	@pipelineid [md_record_written_pipeline_id],
	@jobid [md_transformation_job_id]
FROM
	(select * FROM [stage].[workday_employeedata]where isnumeric(trim([employeeContractHours]))=1 or employeecontracthours is  null)a
	OPTION (LABEL = 'AADPSTDWDEMPDATA');

WITH empdata AS (
	SELECT
		*,
		row_number() OVER (
			PARTITION BY employeeCode
			ORDER BY
				[md_record_ingestion_timestamp] DESC,
				md_record_written_timestamp DESC
		) AS dupcnt
	FROM
		std.workday_employeedata
)
DELETE FROM
	empdata
WHERE
	dupcnt > 1;

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
	@label = 'AADPSTDWDEMPDATA' EXEC meta_ctl.sp_row_count @jobid,
	@step_number,
	@label 
	
	Truncate table [stage].[workday_employeedata];

END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
	@newrec = max(md_record_written_timestamp)
FROM
	std.[workday_employeedata];

SELECT
	@onlydate = CAST(@newrec AS DATE);

DELETE FROM
	std.[workday_employeedata]
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
	'std.sp_workday_employee' AS ErrorProcedure,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() AS Updated_date
END CATCH
END
