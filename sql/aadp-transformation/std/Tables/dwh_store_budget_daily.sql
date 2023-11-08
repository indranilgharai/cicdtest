/****** Object:  Table [std].[dwh_store_budget_daily]    Script Date: 11/20/2022 1:49:09 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[dwh_store_budget_daily]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[budget_yyyymmdd] [varchar](20) NULL,
	[budget_year] [int] NULL,
	[budget_fy] [int] NULL,
	[budget_month] [int] NULL,
	[budget_day] [int] NULL,
	[budget_dow] [varchar](10) NULL,
	[budget_week_no] [int] NULL,
	[sales_budget] float NULL,
	[md_record_ingestion_timestamp] datetime NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](10) NULL,
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


