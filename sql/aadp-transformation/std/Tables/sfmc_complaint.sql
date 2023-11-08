/****** Object:  Table [std].[sfmc_complaint]    Script Date: 4/12/2022 5:55:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[sfmc_complaint]
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
	[IsUnique] [varchar](200) NULL,
	[Domain] [varchar](200) NULL,
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


