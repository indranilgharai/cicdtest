SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[sfmc_journeyactivity]
(
	[Hashkey] [nvarchar](max) NULL,
	[VersionID] [nvarchar](max) NULL,
	[ActivityID] [nvarchar](max) NULL,
	[ActivityName] [nvarchar](max) NULL,
	[ActivityExternalKey] [nvarchar](max) NULL,
	[JourneyActivityObjectID] [nvarchar](max) NULL,
	[ActivityType] [nvarchar](max) NULL,
	[AccountID] [nvarchar](max) NULL,
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