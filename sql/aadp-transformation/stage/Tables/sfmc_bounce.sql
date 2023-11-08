SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[sfmc_bounce]
(
	[HashKey] [nvarchar](max) NULL,
	[SubscriberKey] [nvarchar](max) NULL,
	[SubscriberID] [nvarchar](max) NULL,
	[AccountID] [nvarchar](max) NULL,
	[OYBAccountID] [nvarchar](max) NULL,
	[Domain] [nvarchar](max) NULL,
	[EventDate] [nvarchar](max) NULL,
	[JobID] [nvarchar](max) NULL,
	[ListID] [nvarchar](max) NULL,
	[BatchID] [nvarchar](max) NULL,
	[IsUnique] [nvarchar](max) NULL,
	[TriggererSendDefinitionObjectID] [nvarchar](max) NULL,
	[TriggeredSendCustomerKey] [nvarchar](max) NULL,
	[BounceTypeID] [nvarchar](max) NULL,
	[BounceType] [nvarchar](max) NULL,
	[BounceCategoryID] [nvarchar](max) NULL,
	[BounceCategory] [nvarchar](max) NULL,
	[BounceSubcategoryID] [nvarchar](max) NULL,
	[BounceSubcategory] [nvarchar](max) NULL,
	[Reason] [nvarchar](max) NULL,
	[SMTPCode] [nvarchar](max) NULL,
	[EnhancedStatusCode] [nvarchar](max) NULL,
	[SMTPBounceReason] [nvarchar](max) NULL,
	[SMTPMessage] [nvarchar](max) NULL,
	[DefaultVal] [nvarchar](max) NULL,
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