SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[kepler_incoming]
(
	[sbs_no] [nvarchar](max) NULL,
	[store_no] [nvarchar](max) NULL,
	[traffic_date] [nvarchar](max) NULL,
	[traffic_hour] [nvarchar](max) NULL,
	[store_operating] [nvarchar](max) NULL,
	[outside_traffic_outside_work_hours] [nvarchar](max) NULL,
	[outside_traffic_during_work_hours] [nvarchar](max) NULL,
	[inside] [nvarchar](max) NULL,
	[shopfront_conversion] [nvarchar](max) NULL,
	[dwell_time_total_seconds] [nvarchar](max) NULL,
	[br60_qty] [nvarchar](max) NULL,
	[br120_qty] [nvarchar](max) NULL,
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


