SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_inventory_numbers]
(
	[expiration_Date] [datetime] NULL,
	[external_id] [varchar](200) NULL,
	[internal_id] [varchar](200) NULL,
	[item] [varchar](200) NULL,
	[last_modified_date] [datetime] NULL,
	[memo] [varchar](200) NULL,
	[inventory_number_lot] [nvarchar](200) NULL,
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