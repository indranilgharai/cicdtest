SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[fps_alias]
(
	[item_person_uuid] [varchar](100) NULL,
	[item_email] [nvarchar](100) NULL,
	[item_fps_last_modified] [datetime] NULL,
	[Item_source] [varchar](500) NULL,
	[Item_source_id] [varchar](500) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [item_person_uuid] ),
	CLUSTERED COLUMNSTORE INDEX
)
