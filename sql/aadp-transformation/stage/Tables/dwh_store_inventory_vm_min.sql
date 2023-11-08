SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_store_inventory_vm_min]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[description1] [nvarchar](max) NULL,
	[vm_min] [int] NULL,
	[vm_min_old] [int] NULL,
	[sellable_stock] [int] NULL,
	[sellable_stock_old] [int] NULL,
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


