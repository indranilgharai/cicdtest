SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_transaction_type_list]
(
	[external_id] [varchar](500) NULL,
	[id] [varchar](500) NULL,
	[is_inactive] [varchar](10) NULL,
	[name] [varchar](500) NULL,
	[record_id] [varchar](500) NULL,
	[script_id] [varchar](500) NULL,
	[owner] [varchar](500) NULL,
	[created_date] [datetime] NULL,
	[last_modified_Date] [datetime] NULL,
	[description] [varchar](500) NULL,
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