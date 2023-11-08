SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_locationtype]
(
	[id] [bigint] NULL,
	[name] [nvarchar](max) NULL,
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