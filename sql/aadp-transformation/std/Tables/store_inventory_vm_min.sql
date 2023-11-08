SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[store_inventory_vm_min]
(
	[storeinvkey] [varchar](500) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[description1] [varchar](100) NULL,
	[vm_min] [int] NULL,
	[vm_min_old] [int] NULL,
	[sellable_stock] [int] NULL,
	[sellable_stock_old] [int] NULL,
	[md_record_ingestion_timestamp] [datetime] NOT NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NOT NULL,
	[md_transformation_job_id] [varchar](500) NOT NULL,
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO


