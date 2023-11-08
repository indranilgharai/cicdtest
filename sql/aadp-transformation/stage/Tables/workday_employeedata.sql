SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[workday_employeedata]
(
	[employeeCode] [nvarchar](max) NULL,
	[employeeEmail] [nvarchar](max) NULL,
	[employeeLastName] [nvarchar](max) NULL,
	[employeeFirstName] [nvarchar](max) NULL,
	[employeeStore] [nvarchar](max) NULL,
	[terminationDate] [nvarchar](max) NULL,
	[startDate] [nvarchar](max) NULL,
	[employeeCountry] [nvarchar](max) NULL,
	[employeeJobTitle] [nvarchar](max) NULL,
	[employeeStatus] [nvarchar](max) NULL,
	[employeeContractHours] [nvarchar](max) NULL,
	[employeeStoreName] [nvarchar](max) NULL,
	[employeeLegalName] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


