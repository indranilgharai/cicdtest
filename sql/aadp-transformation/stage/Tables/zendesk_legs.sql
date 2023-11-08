/****** Object:  Table [stage].[zendesk_legs]    Script Date: 4/10/2022 10:00:59 AM ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stage].[zendesk_legs]
(
	[agent_id] [nvarchar](max) NULL,
	[call_id] [nvarchar](max) NULL,
	[completion_status] [nvarchar](max) NULL,
	[consultation_time] [nvarchar](max) NULL,
	[consultation_to] [nvarchar](max) NULL,
	[created_at] [nvarchar](max) NULL,
	[duration] [nvarchar](max) NULL,
	[hold_time] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[quality_issues] [nvarchar](max) NULL,
	[talk_time] [nvarchar](max) NULL,
	[transferred_from] [nvarchar](max) NULL,
	[transferred_to] [nvarchar](max) NULL,
	[type] [nvarchar](max) NULL,
	[updated_at] [nvarchar](max) NULL,
	[user_id] [nvarchar](max) NULL,
	[wrap_up_time] [nvarchar](max) NULL,
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