/****** Object:  Table [std].[zendesk_legs]    Script Date: 24/10/2022 10:00:59 AM ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [std].[zendesk_legs]
(
	[agent_id] [varchar](30) NULL,
	[call_id] [varchar](30) NULL,
	[completion_status] [varchar](100) NULL,
	[consultation_time] [bigint] NULL,
	[consultation_to] [varchar](100) NULL,
	[created_at] [datetime] NULL,
	[duration] [bigint] NULL,
	[hold_time] [bigint] NULL,
	[id] [bigint] NOT NULL ,
	[quality_issues] [varchar](500) NULL,
	[talk_time] [bigint] NULL,
	[transferred_from] [varchar](30) NULL,
	[transferred_to] [varchar](30) NULL,
	[type] [varchar](30) NULL,
	[updated_at] [datetime] NULL,
	[user_id] [varchar](30) NULL,
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
