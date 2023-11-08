/****** Object:  Table [std].[zendesk_calls]    Script Date: 24/10/2022 10:00:59 AM ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [std].[zendesk_calls]
(
	[agent_id] [varchar](30) NULL,
	[completion_status] [varchar](100) NULL,
	[consultation_time] [varchar](30) NULL,
	[created_at] [datetime] NULL,
	[customer_requested_voicemail] [varchar](100) NULL,
	[default_group] [varchar](100) NULL,
	[direction] [varchar](100) NULL,
	[duration] [bigint] NULL,
	[exceeded_queue_time] [varchar](100) NULL,
	[hold_time] [varchar](30) NULL,
	[id] [bigint] NOT NULL,
	[ivr_action] [varchar](100) NULL,
	[ivr_destination_group_name] [varchar](100) NULL,
	[ivr_hops] [varchar](100) NULL,
	[ivr_routed_to] [varchar](100) NULL,
	[ivr_time_spent] [varchar](100) NULL,
	[outside_business_hours] [varchar](100) NULL,
	[quality_issues] [varchar](500) NULL,
	[talk_time] [varchar](100) NULL,
	[ticket_id] [varchar](100) NULL,
	[time_to_answer] [varchar](100) NULL,
	[updated_at] [datetime] NULL,
	[voicemail] [varchar](100) NULL,
	[wait_time] [bigint] NULL,
	[wrap_up_time] [bigint] NULL,
	[md_record_ingestion_timestamp] [datetime] NOT NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](30) NULL,
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
