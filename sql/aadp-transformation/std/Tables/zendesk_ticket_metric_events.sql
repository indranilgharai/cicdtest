/****** Object:  Table [std].[zendesk_ticket_metric_events]    Script Date: 4/29/2022 7:10:21 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[zendesk_ticket_metric_events]
(
	[deleted] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[instance_id] [nvarchar](max) NULL,
	[metric] [nvarchar](max) NULL,
	[sla] [nvarchar](max) NULL,
	[status] [nvarchar](max) NULL,
	[ticket_id] [nvarchar](max) NULL,
	[time] [nvarchar](max) NULL,
	[type] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO