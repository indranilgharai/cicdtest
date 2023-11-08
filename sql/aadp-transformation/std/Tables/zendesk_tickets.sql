SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[zendesk_tickets]
(
	[assignee_id] [bigint] NULL,
	[brand_id] [bigint] NULL,
	[created_at] [datetime] NOT NULL,
	[generated_timestamp] [bigint] NULL,
	[id] [int] NOT NULL,
	[priority] [nvarchar](30) NULL,
	[requester_id] [bigint] NULL,
	[status] [nvarchar](30) NULL,
	[submitter_id] [bigint] NULL,
	[tags] [nvarchar](max) NULL,
	[ticket_form_id] [bigint] NULL,
	[type] [nvarchar](30) NULL,
	[updated_at] [datetime] NOT NULL,
	[agent_wait_time_in_minutes_business] [bigint] NULL,
	[agent_wait_time_in_minutes_calendar] [bigint] NULL,
	[assigned_at] [datetime] NULL,
	[assignee_stations] [int] NULL,
	[assignee_updated_at] [datetime] NULL,
	[metric_created_at] [datetime] NULL,
	[first_resolution_time_in_minutes_business] [bigint] NULL,
	[first_resolution_time_in_minutes_calendar] [bigint] NULL,
	[full_resolution_time_in_minutes_business] [bigint] NULL,
	[full_resolution_time_in_minutes_calendar] [bigint] NULL,
	[initially_assigned_at] [datetime] NULL,
	[on_hold_time_in_minutes_business] [bigint] NULL,
	[on_hold_time_in_minutes_calendar] [bigint] NULL,
	[reopens] [int] NULL,
	[replies] [int] NULL,
	[reply_time_in_minutes_business] [bigint] NULL,
	[reply_time_in_minutes_calendar] [bigint] NULL,
	[reply_time_in_seconds_business] [bigint] NULL,
	[reply_time_in_seconds_calendar] [bigint] NULL,
	[requester_updated_at] [datetime] NULL,
	[solved_at] [datetime] NULL,
	[requester_wait_time_in_minutes_business] [bigint] NULL,
	[requester_wait_time_in_minutes_calendar] [bigint] NULL,
	[channel] [nvarchar](100) NULL,
	[integration_service_instance_name] [nvarchar](500) NULL,
	[registered_integration_service_name] [nvarchar](500) NULL,
	[md_record_ingestion_timestamp] [datetime] NOT NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](500) NULL,
	[md_source_system] [nvarchar](30) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [nvarchar](500) NULL
)
WITH
(
	DISTRIBUTION = HASH(id),
	HEAP
)
GO