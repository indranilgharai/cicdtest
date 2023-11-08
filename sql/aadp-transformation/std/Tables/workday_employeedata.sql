/****** Object:  Table [std].[workday_employeedata]    Script Date: 12/1/2022 7:45:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[workday_employeedata]
(
	[employeeCode] [varchar](100) NULL,
	[employeeEmail] [varchar](500) NULL,
	[employeeLastName] [varchar](100) NULL,
	[employeeFirstName] [varchar](100) NULL,
	[employeeStore] [varchar](100) NULL,
	[terminationDate] [varchar](100) NULL,
	[startDate] [varchar](100) NULL,
	[employeeCountry] [varchar](50) NULL,
	[employeeJobTitle] [varchar](500) NULL,
	[employeeStatus] [varchar](200) NULL,
	[employeeContractHours] [float] NULL,
	[employeeStoreName] [varchar](500) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = hash(employeeCode),
	HEAP
)
GO
