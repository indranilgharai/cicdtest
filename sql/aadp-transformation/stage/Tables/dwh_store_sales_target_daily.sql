SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_store_sales_target_daily]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[store_name] [nvarchar](max) NULL,
	[yyyymmdd] [nvarchar](max) NULL,
	[daily_target] [float] NULL,
	[available] [int] NULL,
	[userid] [nvarchar](max) NULL,
	[date_unix] [nvarchar](max) NULL,
	[year] [int] NULL,
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


