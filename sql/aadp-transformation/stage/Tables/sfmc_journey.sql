SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[sfmc_journey]
(
	[Hashkey] [nvarchar](max) NULL,
	[VersionID] [nvarchar](max) NULL,
	[JourneyID] [nvarchar](max) NULL,
	[JourneyName] [nvarchar](max) NULL,
	[VersionNumber] [nvarchar](max) NULL,
	[CreatedDate] [nvarchar](max) NULL,
	[LastPublishedDate] [nvarchar](max) NULL,
	[ModifiedDate] [nvarchar](max) NULL,
	[JourneyStatus] [nvarchar](max) NULL,
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