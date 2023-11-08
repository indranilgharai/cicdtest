SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [std].[exchange_rate_x]
(
	[sbs_no] [varchar](10) NULL,
	[month_no] [int] NULL,
	[year] [int] NULL,
	[fy] [int] NULL,
	[ex_rate] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO