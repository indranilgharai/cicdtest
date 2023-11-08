SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_ship_status]
(
	[external_id] [varchar](100) NULL,
	[internal_id] [varchar](100) NULL,
	[is_inactive] [char](2) NULL,
	[name] [varchar](200) NULL,
	[record_id] [varchar](200) NULL,
	[script_id] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [internal_id] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO


