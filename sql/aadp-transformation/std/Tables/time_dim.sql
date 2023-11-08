SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[time_dim]
(
	[hr_24] [int] NULL,
	[hr_12] [int] NULL,
	[business_hour] [varchar](3) NOT NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](30) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO