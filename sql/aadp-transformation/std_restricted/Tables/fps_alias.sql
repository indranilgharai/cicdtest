/****** Object:  Table [std_restricted].[fps_alias]    Script Date: 3/31/2022 7:05:30 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std_restricted].[fps_alias]
(
	[Item_person_uuid] [varchar](100) NULL,
	[Item_phone] [varchar](100) NULL,
	[Item_email] [varchar](100) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [Item_person_uuid] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO

