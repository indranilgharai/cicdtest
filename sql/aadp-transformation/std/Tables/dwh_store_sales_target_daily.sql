SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[dwh_store_sales_target_daily]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[store_name] [varchar](300) NULL,
	[yyyymmdd] [varchar](20) NULL,
	[daily_target] [float] NULL,
	[available] [int] NULL,
	[userid] [varchar](200) NULL,
	[date_unix] [varchar](200) NULL,
	[year] [int] NULL,
	[md_record_ingestion_timestamp] datetime NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](50) NULL,
	[md_record_written_timestamp] datetime NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


