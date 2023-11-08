/****** Object:  View [cons_reference].[dim_consultant_view]    Script Date: 12/4/2022 9:38:48 AM ******/
SET
  ANSI_NULLS ON
GO
SET
  QUOTED_IDENTIFIER ON
GO
  CREATE VIEW [cons_reference].[dim_consultant_view] AS
SELECT
  [employeeCode] EmployeeID,
  [employeeEmail],
  [employeeLastName],
  [employeeFirstName],
  [employeeStore] as homeStore_location_code,
  [terminationDate] EmploymentEndDate,
  [startDate],
  [employeeCountry],
  [employeeJobTitle] BusinessTitle,
  [employeeStatus] TimeType,
  [employeeStatus] WorkerType,
  [employeeContractHours] as ScheduledWeeklyHours,
  [employeeStoreName] "Location",
  wd.[md_record_written_timestamp] as wd_md_record_written_timestamp,
  wd.[md_record_written_pipeline_id] as wd_md_record_written_pipeline_id,
  wd.[md_transformation_job_id] as wd_md_transformation_job_id
FROM
  [std].[workday_employeedata] wd;

GO