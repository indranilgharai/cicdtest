SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [std].[zendesk_contact_reason_mapping]
(
	[ticket_form_id] [bigint] NULL,
	[ticket_form_description] [varchar](100) NULL,
	[contact_reason] [varchar](100) NULL,
	[enq_type_description] [varchar](100) NULL,
	[contact_reason_enq_type] [varchar](100) NULL,
	[contact_reason_details1] [varchar](100) NULL,
	[contact_reason_details2] [varchar](100) NULL,
	[Year] [varchar](100) NULL,
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
