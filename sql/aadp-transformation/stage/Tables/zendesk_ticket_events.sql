/****** Object:  Table [stage].[zendesk_ticket_events]    Script Date: 4/29/2022 7:02:30 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[zendesk_ticket_events]
(
	[child_events] [nvarchar](max) NULL,
	[created_at] [nvarchar](max) NULL,
	[event_type] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[merged_ticket_ids] [nvarchar](max) NULL,
	[system] [nvarchar](max) NULL,
	[ticket_id] [nvarchar](max) NULL,
	[timestamp] [nvarchar](max) NULL,
	[updater_id] [nvarchar](max) NULL,
	[via] [nvarchar](max) NULL,
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