/****** Object:  Table [std_restricted].[sfmc_subscribers]    Script Date: 3/31/2022 7:07:34 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std_restricted].[sfmc_subscribers]
(
	[SubscriberID] [varchar](100) NULL,
	[EmailAddress] [varchar](100) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [SubscriberID] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO

