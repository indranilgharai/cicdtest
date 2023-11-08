SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[dwh_store_budget_weekly]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[week_no] [int] NULL,
	[year] [int] NULL,
	[sales_budget] [float] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO


