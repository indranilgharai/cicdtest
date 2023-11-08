SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_exchange_rate]
(
	[sbs_no] [nvarchar](max) NULL,
	[month_no] [nvarchar](max) NULL,
	[year] [nvarchar](max) NULL,
	[fy] [nvarchar](max) NULL,
	[ex_rate] [float] NULL,
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


