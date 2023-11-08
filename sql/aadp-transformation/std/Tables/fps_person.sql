/****** Object:  Table [std].[fps_person_dev]    Script Date: 3/22/2022 6:50:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[fps_person]
(
	[person_uuid] [varchar](100) NULL,
	[customer_group_id] [varchar](100) NULL,
	[email] [varchar](100) NULL,
	[home_store] [varchar](100) NULL,
	[fps_created] [datetime] NULL,
	[fps_last_modified] [datetime] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [person_uuid] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO