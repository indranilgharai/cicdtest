SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[sfmc_job]
(
	[HashKey] [nvarchar](max) NULL,
	[JobID] [nvarchar](max) NULL,
	[EmailID] [nvarchar](max) NULL,
	[AccountID] [nvarchar](max) NULL,
	[AccountUserID] [nvarchar](max) NULL,
	[FromName] [nvarchar](max) NULL,
	[FromEmail] [nvarchar](max) NULL,
	[SchedTime] [nvarchar](max) NULL,
	[PickupTime] [nvarchar](max) NULL,
	[DeliveredTime] [nvarchar](max) NULL,
	[EventID] [nvarchar](max) NULL,
	[IsMultipart] [nvarchar](max) NULL,
	[JobType] [nvarchar](max) NULL,
	[JobStatus] [nvarchar](max) NULL,
	[ModifiedBy] [nvarchar](max) NULL,
	[ModifiedDate] [nvarchar](max) NULL,
	[EmailName] [nvarchar](max) NULL,
	[EmailSubject] [nvarchar](max) NULL,
	[IsWrapped] [nvarchar](max) NULL,
	[TestEmailAddr] [nvarchar](max) NULL,
	[Category] [nvarchar](max) NULL,
	[BccEmail] [nvarchar](max) NULL,
	[OriginalSchedTime] [nvarchar](max) NULL,
	[CreatedDate] [nvarchar](max) NULL,
	[CharacterSet] [nvarchar](max) NULL,
	[IPAddress] [nvarchar](max) NULL,
	[SalesForceTotalSubscriberCount] [nvarchar](max) NULL,
	[SalesForceErrorSubscriberCount] [nvarchar](max) NULL,
	[SendType] [nvarchar](max) NULL,
	[DynamicEmailSubject] [nvarchar](max) NULL,
	[SuppressTracking] [nvarchar](max) NULL,
	[SendClassificationType] [nvarchar](max) NULL,
	[SendClassification] [nvarchar](max) NULL,
	[ResolveLinksWithCurrentData] [nvarchar](max) NULL,
	[EmailSendDefinition] [nvarchar](max) NULL,
	[DeduplicateByEmail] [nvarchar](max) NULL,
	[TriggererSendDefinitionObjectID] [nvarchar](max) NULL,
	[TriggeredSendCustomerKey] [nvarchar](max) NULL,
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