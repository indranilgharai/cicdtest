

/****** Object:  Table [std].[sfmc_subscribers]    Script Date: 4/12/2022 7:40:53 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[sfmc_subscribers]
(
	[SubscriberID] [bigint] NULL,
	[DateUndeliverable] [datetime] NULL,
	[DateJoined] [datetime] NULL,
	[DateUnsubscribed] [datetime] NULL,
	[BounceCount] [bigint] NULL,
	[SubscriberKey] [varchar](200) NULL,
	[SubscriberType] [varchar](200) NULL,
	[Status] [varchar](200) NULL,
	[Locale] [varchar](200) NULL,
	[brand] [varchar](200) NULL,
	[aesop_country] [varchar](200) NULL,
	[aesop_language] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[Domain] [varchar](200) NULL,
	[EmailAddress] [varchar](200) NULL,
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


