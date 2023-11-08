SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_shipping_method]
(
	[external_id] [varchar](200) NULL,
	[id] [varchar](200) NULL,
	[is_inactive] [varchar](200) NULL,
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
	DISTRIBUTION = HASH ( [id] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO