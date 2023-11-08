SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[sfmc_unsubscribe]
(
	[HashKey] [nvarchar](max) NULL,
	[SubscriberKey] [nvarchar](max) NULL,
	[EventDate] [nvarchar](max) NULL,
	[IsUnique] [nvarchar](max) NULL,
	[AccountID] [nvarchar](max) NULL,
	[OYBAccountID] [nvarchar](max) NULL,
	[SubscriberID] [nvarchar](max) NULL,
	[JobID] [nvarchar](max) NULL,
	[ListID] [nvarchar](max) NULL,
	[BatchID] [nvarchar](max) NULL,
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