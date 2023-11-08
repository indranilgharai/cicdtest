SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_producttypecategory]
(
	[created] [datetime2](7) NULL,
	[custrecord_ec_parent_item_category] [nvarchar](max) NULL,
	[custrecord_ec_ptc_code] [nvarchar](max) NULL,
	[externalid] [nvarchar](max) NULL,
	[id] [bigint] NULL,
	[isinactive] [nvarchar](max) NULL,
	[lastmodified] [datetime2](7) NULL,
	[name] [nvarchar](max) NULL,
	[owner] [bigint] NULL,
	[recordid] [bigint] NULL,
	[scriptid] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO