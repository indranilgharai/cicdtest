
/****** Object:  Table [std].[sfmc_bounce]    Script Date: 4/12/2022 5:26:48 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   TABLE [std].[sfmc_bounce]
(
	[HashKey] [varchar](200) NULL,
	[SubscriberKey] [varchar](200) NULL,
	[SubscriberID] [bigint] NULL,
	[AccountID] [bigint] NULL,
	[OYBAccountID] [bigint] NULL,
	[Domain] [varchar](200) NULL,
	[EventDate] [datetime] NULL,
	[JobID] [bigint] NULL,
	[ListID] [bigint] NULL,
	[BatchID] [bigint] NULL,
	[IsUnique] [varchar](200) NULL,
	[TriggererSendDefinitionObjectID] [varchar](200) NULL,
	[TriggeredSendCustomerKey] [varchar](200) NULL,
	[BounceTypeID] [bigint] NULL,
	[BounceType] [varchar](200) NULL,
	[BounceCategoryID] [bigint] NULL,
	[BounceCategory] [varchar](200) NULL,
	[BounceSubcategoryID] [bigint] NULL,
	[BounceSubcategory] [varchar](200) NULL,
	[Reason] [varchar](200) NULL,
	[SMTPCode] [bigint] NULL,
	[EnhancedStatusCode] [varchar](200) NULL,
	[SMTPBounceReason] [varchar](200) NULL,
	[SMTPMessage] [varchar](200) NULL,
	[DefaultVal] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
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



