SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[cegid_bundles]
( 
	[header_correlationId] [nvarchar](max)  NULL,
	[header_messageType] [nvarchar](max)  NULL,
	[header_method] [nvarchar](max)  NULL,
	[header_sourceSystem] [nvarchar](max)  NULL,
	[body_filename] [nvarchar](max)  NULL,
	[bundle_code] [nvarchar](max)  NULL,
	[bundle_description] [nvarchar](max)  NULL,
	[bundle_barcode] [nvarchar](max)  NULL,
	[sku_line_no] [nvarchar](max)  NULL,
	[sku_code] [nvarchar](max)  NULL,
	[sku_description] [nvarchar](max)  NULL,
	[sku_barcode] [nvarchar](max)  NULL,
	[sku_qty] [nvarchar](max)  NULL,
	[bundle_date_creation] [nvarchar](max)  NULL,
	[bundle_date_modif] [nvarchar](max)  NULL,
	[bundle_price] [nvarchar](max)  NULL,
	[bundle_currency] [nvarchar](max)  NULL,
	[sku_price] [nvarchar](max)  NULL,
	[AADP_source_filename] [nvarchar](max)  NULL,
	[md_record_ingestion_timestamp] [nvarchar](max)  NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max)  NULL,
	[md_source_system] [nvarchar](max)  NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO