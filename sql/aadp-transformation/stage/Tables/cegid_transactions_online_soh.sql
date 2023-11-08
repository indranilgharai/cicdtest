SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[cegid_transactions_online_soh]
(
	[header_correlationId] [nvarchar](max) NULL,
	[header_messageType] [nvarchar](max) NULL,
	[header_method] [nvarchar](max) NULL,
	[header_sourceSystem] [nvarchar](max) NULL,
	[body_filename] [nvarchar](max) NULL,
	[storeWarehouseCode] [nvarchar](max) NULL,
	[itemCode] [nvarchar](max) NULL,
	[physicalInventory] [nvarchar](max) NULL,
	[qtyReserved] [nvarchar](max) NULL,
	[qtyAvailable] [nvarchar](max) NULL,
	[qtyInTransitWarehouse] [nvarchar](max) NULL,
	[qtyInTransitStore] [nvarchar](max) NULL,
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


