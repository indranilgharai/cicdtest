SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[cegid_bundles]
(
	[bundle_code] [varchar](100) NULL,
	[bundle_description] [varchar](100) NULL,
	[bundle_barcode] [varchar](100) NULL,
	[sku_line_no] [varchar](100) NULL,
	[sku_code] [varchar](100) NULL,
	[sku_description] [varchar](100) NULL,
	[sku_barcode] [varchar](100) NULL,
	[sku_qty] [varchar](100) NULL,
	[bundle_date_creation] [varchar](100) NULL,
	[bundle_date_modif] [varchar](100) NULL,
	[bundle_price] [float] NULL,
	[bundle_currency] [varchar](100) NULL,
	[sku_price] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH(bundle_code),
	CLUSTERED COLUMNSTORE INDEX
);