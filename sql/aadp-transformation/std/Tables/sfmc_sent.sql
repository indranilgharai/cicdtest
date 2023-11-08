

/****** Object:  Table [std].[sfmc_sent]    Script Date: 4/12/2022 7:30:24 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[sfmc_sent]
(
	[HashKey] [varchar](200) NULL,
	[AccountID] [bigint] NULL,
	[OYBAccountID] [bigint] NULL,
	[JobID] [bigint] NULL,
	[ListID] [bigint] NULL,
	[BatchID] [bigint] NULL,
	[SubscriberID] [bigint] NULL,
	[SubscriberKey] [varchar](200) NULL,
	[EventDate] [datetime] NULL,
	[Domain] [varchar](200) NULL,
	[TriggererSendDefinitionObjectID] [varchar](200) NULL,
	[TriggeredSendCustomerKey] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


