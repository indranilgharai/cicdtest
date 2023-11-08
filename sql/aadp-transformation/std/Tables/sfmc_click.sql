

/****** Object:  Table [std].[sfmc_click]    Script Date: 4/12/2022 5:47:33 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [std].[sfmc_click]
(
	[HashKey] [varchar](200) NULL,
	[AccountID] [BIGINT] NULL,
	[OYBAccountID] [BIGINT] NULL,
	[JobID] [BIGINT] NULL,
	[ListID] [BIGINT] NULL,
	[BatchID] [BIGINT] NULL,
	[SubscriberID] [BIGINT] NULL,
	[SubscriberKey] [varchar](200) NULL,
	[EventDate] [datetime] NULL,
	[Domain] [varchar](200) NULL,
	[URL] [nvarchar](2000) NULL,
	[LinkName] [nvarchar](2000) NULL,
	[LinkContent] [nvarchar](2000) NULL,
	[IsUnique] [varchar](200) NULL,
	[TriggererSendDefinitionObjectID] [varchar](200) NULL,
	[TriggeredSendCustomerKey] [varchar](200) NULL,
	[EventDateText] [datetime] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NOT NULL,
	[md_transformation_job_id] [varchar](500) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


