/****** Object:  Table [std].[fps_person_alias_dev]    Script Date: 3/22/2022 6:54:17 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[fps_person_alias]
(
	[customer_id] [varchar](100) NULL,
	[customer_group_id] [varchar](100) NULL,
	[home_store] [varchar](100) NULL,
	[create_date] [date] NULL,
	[last_modified] [date] NULL,
	[email] [varchar](1000) NULL,
	[is_aesop_employee] [varchar](10) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [customer_id] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO
