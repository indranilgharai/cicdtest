/****** Object:  Table [stage].[zendesk_calls]    Script Date: 4/10/2022 10:00:59 AM ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stage].[zendesk_calls]
(
	[agent_id] [nvarchar](max) NULL,
	[completion_status] [nvarchar](max) NULL,
	[consultation_time] [nvarchar](max) NULL,
	[created_at] [nvarchar](max) NULL,
	[customer_requested_voicemail] [nvarchar](max) NULL,
	[default_group] [nvarchar](max) NULL,
	[direction] [nvarchar](max) NULL,
	[duration] [nvarchar](max) NULL,
	[exceeded_queue_time] [nvarchar](max) NULL,
	[hold_time] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[ivr_action] [nvarchar](max) NULL,
	[ivr_destination_group_name] [nvarchar](max) NULL,
	[ivr_hops] [nvarchar](max) NULL,
	[ivr_routed_to] [nvarchar](max) NULL,
	[ivr_time_spent] [nvarchar](max) NULL,
	[outside_business_hours] [nvarchar](max) NULL,
	[quality_issues] [nvarchar](max) NULL,
	[talk_time] [nvarchar](max) NULL,
	[ticket_id] [nvarchar](max) NULL,
	[time_to_answer] [nvarchar](max) NULL,
	[updated_at] [nvarchar](max) NULL,
	[voicemail] [nvarchar](max) NULL,
	[wait_time] [nvarchar](max) NULL,
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


