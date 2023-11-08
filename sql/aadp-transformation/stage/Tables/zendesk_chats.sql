/****** Object:  Table [stage].[zendesk_chats]    Script Date: 4/27/2022 7:14:38 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[zendesk_chats]
(
	[abandon_time] [nvarchar](max) NULL,
	[agent_ids] [nvarchar](max) NULL,
	[agent_names] [nvarchar](max) NULL,
	[comment] [nvarchar](max) NULL,
	[conversions] [nvarchar](max) NULL,
	[count] [nvarchar](max) NULL,
	[deleted] [nvarchar](max) NULL,
	[department_id] [nvarchar](max) NULL,
	[department_name] [nvarchar](max) NULL,
	[dropped] [nvarchar](max) NULL,
	[duration] [nvarchar](max) NULL,
	[end_timestamp] [nvarchar](max) NULL,
	[engagements] [nvarchar](max) NULL,
	[history] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[message] [nvarchar](max) NULL,
	[missed] [nvarchar](max) NULL,
	[proactive] [nvarchar](max) NULL,
	[rating] [nvarchar](max) NULL,
	[referrer_search_engine] [nvarchar](max) NULL,
	[referrer_search_terms] [nvarchar](max) NULL,
	[response_time] [nvarchar](max) NULL,
	[session] [nvarchar](max) NULL,
	[skills_fulfilled] [nvarchar](max) NULL,
	[skills_requested] [nvarchar](max) NULL,
	[started_by] [nvarchar](max) NULL,
	[tags] [nvarchar](max) NULL,
	[timestamp] [nvarchar](max) NULL,
	[triggered] [nvarchar](max) NULL,
	[triggered_response] [nvarchar](max) NULL,
	[type] [nvarchar](max) NULL,
	[unread] [nvarchar](max) NULL,
	[update_timestamp] [nvarchar](max) NULL,
	[visitor] [nvarchar](max) NULL,
	[webpath] [nvarchar](max) NULL,
	[zendesk_ticket_id] [nvarchar](max) NULL,
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