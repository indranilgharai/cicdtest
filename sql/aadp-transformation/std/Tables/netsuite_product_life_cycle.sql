SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_product_life_cycle]
(
	[item] [varchar](200) NULL,
	[available_date] [datetime] NULL,
	[country] [varchar](200) NULL,
	[country_code] [varchar](10) NULL,
	[created] [datetime] NULL,
	[discontinued_date] [datetime] NULL,
	[external_id] [varchar](200) NULL,
	[last_modified] [datetime] NULL,
	[name] [varchar](200) NULL,
	[node] [varchar](200) NULL,
	[obsolete_date] [datetime] NULL,
	[owner] [varchar](200) NULL,
	[phase_out_date] [datetime] NULL,
	[script_id] [varchar](200) NULL,
	[plc_status] [varchar](200) NULL,
	[id] [varchar](200) NULL,
	[is_inactive] [char](2) NULL,
	[alt_name] [varchar](200) NULL,
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


