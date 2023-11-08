SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_shippingcontainerdetail]
(
	[created] [datetime2](7) NULL,
	[custrecord_ec_scd_amount] [float] NULL,
	[custrecord_ec_scd_batches_origin] [nvarchar](max) NULL,
	[custrecord_ec_scd_batches_received] [nvarchar](max) NULL,
	[custrecord_ec_scd_batches_sent] [nvarchar](max) NULL,
	[custrecord_ec_scd_compl_item_receipt] [bigint] NULL,
	[custrecord_ec_scd_description] [nvarchar](max) NULL,
	[custrecord_ec_scd_gross_amount] [float] NULL,
	[custrecord_ec_scd_item] [bigint] NULL,
	[custrecord_ec_scd_item_fulfillment] [bigint] NULL,
	[custrecord_ec_scd_item_receipt] [nvarchar](max) NULL,
	[custrecord_ec_scd_line_number] [bigint] NULL,
	[custrecord_ec_scd_over_batches] [nvarchar](max) NULL,
	[custrecord_ec_scd_over_quantity] [bigint] NULL,
	[custrecord_ec_scd_over_transaction] [bigint] NULL,
	[custrecord_ec_scd_price] [float] NULL,
	[custrecord_ec_scd_purchase_order] [bigint] NULL,
	[custrecord_ec_scd_quantity_received] [bigint] NULL,
	[custrecord_ec_scd_quantity_sent] [bigint] NULL,
	[custrecord_ec_scd_sales_order] [bigint] NULL,
	[custrecord_ec_scd_shipping_container] [bigint] NULL,
	[custrecord_ec_scd_tax_amount] [float] NULL,
	[custrecord_ec_scd_tax_percent] [float] NULL,
	[custrecord_ec_scd_under_batches] [nvarchar](max) NULL,
	[custrecord_ec_scd_under_quantity] [bigint] NULL,
	[custrecord_ec_scd_under_transaction] [bigint] NULL,
	[custrecord_ec_scd_vendor_bill] [bigint] NULL,
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


