/****** Object:  Table [stage].[zendesk_tickets]    Script Date: 4/27/2022 7:15:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[zendesk_tickets]
(

	[assignee_id] [nvarchar](max) NULL,
	[brand_id] [nvarchar](max) NULL,
	[created_at] [nvarchar](max) NULL,
	[generated_timestamp] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[priority] [nvarchar](max) NULL,
	[requester_id] [nvarchar](max) NULL,
	[status] [nvarchar](max) NULL,
	[submitter_id] [nvarchar](max) NULL,
	[tags] [nvarchar](max) NULL,
	[ticket_form_id] [nvarchar](max) NULL,
	[type] [nvarchar](max) NULL,
	[updated_at] [nvarchar](max) NULL,
	[agent_wait_time_in_minutes_business] [nvarchar](max) NULL,
	[agent_wait_time_in_minutes_calendar] [nvarchar](max) NULL,
	[assigned_at] [nvarchar](max) NULL,
	[assignee_stations] [nvarchar](max) NULL,
	[assignee_updated_at] [nvarchar](max) NULL,
	[metric_created_at] [nvarchar](max) NULL,
	[first_resolution_time_in_minutes_business] [nvarchar](max) NULL,
	[first_resolution_time_in_minutes_calendar] [nvarchar](max) NULL,
	[full_resolution_time_in_minutes_business] [nvarchar](max) NULL,
	[full_resolution_time_in_minutes_calendar] [nvarchar](max) NULL,
	[initially_assigned_at] [nvarchar](max) NULL,
	[on_hold_time_in_minutes_business] [nvarchar](max) NULL,
	[on_hold_time_in_minutes_calendar] [nvarchar](max) NULL,
	[reopens] [nvarchar](max) NULL,
	[replies] [nvarchar](max) NULL,
	[reply_time_in_minutes_business] [nvarchar](max) NULL,
	[reply_time_in_minutes_calendar] [nvarchar](max) NULL,
	[reply_time_in_seconds_business] [nvarchar](max) NULL,
	[reply_time_in_seconds_calendar] [nvarchar](max) NULL,
	[requester_updated_at] [nvarchar](max) NULL,
	[solved_at] [nvarchar](max) NULL,
	[requester_wait_time_in_minutes_business] [nvarchar](max) NULL,
	[requester_wait_time_in_minutes_calendar] [nvarchar](max) NULL,
	[channel] [nvarchar](max) NULL,
	[integration_service_instance_name] [nvarchar](max) NULL,
	[registered_integration_service_name] [nvarchar](max) NULL,
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