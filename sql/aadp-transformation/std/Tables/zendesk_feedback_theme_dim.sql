SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [std].[zendesk_feedback_theme_dim]
(
	[ticket_id] [int] NULL,
	[feedback_theme] [varchar](500) NULL,
	[md_record_ingestion_timestamp] [datetime] NOT NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](30) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL

)
WITH
(  DISTRIBUTION = HASH ( [ticket_id] ),
	CLUSTERED INDEX
	(
		[ticket_id] ASC
	)
)
GO
