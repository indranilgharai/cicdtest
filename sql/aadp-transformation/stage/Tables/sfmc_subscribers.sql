SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[sfmc_subscribers]
(
	[SubscriberID] [nvarchar](max) NULL,
	[DateUndeliverable] [nvarchar](max) NULL,
	[DateJoined] [nvarchar](max) NULL,
	[DateUnsubscribed] [nvarchar](max) NULL,
	[BounceCount] [nvarchar](max) NULL,
	[SubscriberKey] [nvarchar](max) NULL,
	[SubscriberType] [nvarchar](max) NULL,
	[Status] [nvarchar](max) NULL,
	[Locale] [nvarchar](max) NULL,
	[brand] [nvarchar](max) NULL,
	[aesop_country] [nvarchar](max) NULL,
	[aesop_language] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL,
	[Domain] [nvarchar](max) NULL,
	[EmailAddress] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO