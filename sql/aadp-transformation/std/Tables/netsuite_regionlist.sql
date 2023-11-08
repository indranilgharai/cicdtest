SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_regionlist]
(
	[externalid] [nvarchar](200) NULL,
	[id] [bigint] NULL,
	[isinactive] [nvarchar](200) NULL,
	[name] [nvarchar](500) NULL,
	[recordid] [bigint] NULL,
	[scriptid] [nvarchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](500) NULL,
	[md_source_system] [nvarchar](100) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NOT NULL,
	[md_transformation_job_id] [varchar](500) NOT NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO


