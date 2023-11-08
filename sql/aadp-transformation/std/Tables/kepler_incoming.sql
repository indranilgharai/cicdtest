SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[kepler_incoming]
(
	incoming_key varchar(50) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[traffic_date] [date] NULL,
	[traffic_hour] [int] NULL,
	[store_operating] [int] NULL,
	[outside_traffic_outside_work_hours] [int] NULL,
	[outside_traffic_during_work_hours] [int] NULL,
	[inside] [int] NULL,
	[shopfront_conversion] [float] NULL,
	[dwell_time_total_seconds] [bigint] NULL,
	[br60_qty] [int] NULL,
	[br120_qty] [int] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = HASH(incoming_key),
	CLUSTERED COLUMNSTORE INDEX
)
GO


