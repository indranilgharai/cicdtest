
/****** Object:  Table [std].[sfmc_journeyactivity]    Script Date: 4/12/2022 7:15:12 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[sfmc_journeyactivity]
(
	[Hashkey] [varchar](200) NULL,
	[VersionID] [varchar](200) NULL,
	[ActivityID] [varchar](200) NULL,
	[ActivityName] [varchar](200) NULL,
	[ActivityExternalKey] [varchar](200) NULL,
	[JourneyActivityObjectID] [varchar](200) NULL,
	[ActivityType] [varchar](200) NULL,
	[AccountID] [bigint] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
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


