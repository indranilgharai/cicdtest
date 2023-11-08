SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_product_type_category]
(
	[created] [datetime] NULL,
	[custrecord_ec_parent_item_category] [nvarchar](500) NULL,
	[custrecord_ec_ptc_code] [nvarchar](500) NULL,
	[externalid] [nvarchar](500) NULL,
	[id] [bigint] NULL,
	[isinactive] [nvarchar](500) NULL,
	[lastmodified] [datetime] NULL,
	[name] [nvarchar](500) NULL,
	[owner] [bigint] NULL,
	[recordid] [bigint] NULL,
	[scriptid] [nvarchar](500) NULL,
	[md_record_written_timestamp] [nvarchar](500) NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [nvarchar](500) NULL
)
WITH
(
DISTRIBUTION = REPLICATE,
HEAP
)
GO