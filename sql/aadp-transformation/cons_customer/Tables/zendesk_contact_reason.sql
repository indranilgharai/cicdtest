SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [cons_customer].[zendesk_contact_reason]
(
	[ticket_id] [int] NULL,
	[contact_reason] [varchar](100) NULL,
	[contact_reason_enq_type] [varchar](100) NULL,
	[contact_reason_details1] [varchar](500) NULL,
	[contact_reason_details2] [varchar](500) NULL,
	[md_record_written_timestamp] [datetime] NULL,
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
