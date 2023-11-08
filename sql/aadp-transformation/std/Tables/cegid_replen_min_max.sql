SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[cegid_replen_min_max]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[item] [varchar](100) NULL,
	[min_old] [int] NULL,
	[min_new] [int] NULL,
	[max_old] [int] NULL,
	[max_new] [int] NULL,
	[created_user] [varchar](100) NULL,
	[created_date] [datetime] NULL,
	[export_user] [varchar](100) NULL,
	[export_date] [datetime] NULL,
	[export_status] [char](2) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [item] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO