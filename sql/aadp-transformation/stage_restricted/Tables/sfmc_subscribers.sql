/****** Object:  Table [stage_restricted].[sfmc_subscribers]    Script Date: 3/31/2022 7:00:16 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage_restricted].[sfmc_subscribers]
(
	[SubscriberID] [nvarchar](max) NULL,
	[EmailAddress] [nvarchar](max) NULL,
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

