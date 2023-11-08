SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_store_budget_daily]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[budget_yyyymmdd] [nvarchar](max) NULL,
	[budget_year] [int] NULL,
	[budget_fy] [int] NULL,
	[budget_month] [nvarchar](max) NULL,
	[budget_day] [nvarchar](max) NULL,
	[budget_dow] [nvarchar](max) NULL,
	[budget_week_no] [nvarchar](max) NULL,
	[sales_budget] [float] NULL,
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


