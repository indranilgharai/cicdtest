/****** Object:  Table [stage].[zendesk_ticket_metric_events]    Script Date: 4/29/2022 7:01:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[zendesk_ticket_metric_events]
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
	[md_source_system] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


