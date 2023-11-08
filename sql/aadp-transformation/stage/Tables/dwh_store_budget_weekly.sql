SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_store_budget_weekly]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[week_no] [nvarchar](max) NULL,
	[year] [int] NULL,
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


