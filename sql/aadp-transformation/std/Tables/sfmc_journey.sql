

/****** Object:  Table [std].[sfmc_journey]    Script Date: 4/12/2022 7:12:18 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[sfmc_journey]
(
	[Hashkey] [varchar](200) NULL,
	[VersionID] [varchar](200) NULL,
	[JourneyID] [varchar](200) NULL,
	[JourneyName] [varchar](200) NULL,
	[VersionNumber] [varchar](200) NULL,
	[CreatedDate] [datetime] NULL,
	[LastPublishedDate] [datetime] NULL,
	[ModifiedDate] [datetime] NULL,
	[JourneyStatus] [varchar](200) NULL,
	[AccountID] [bigint] NULL,
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


