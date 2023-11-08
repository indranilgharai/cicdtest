/****** Modified: Added 'bundle_sku_line_no' and 'bundle_sku_code'    Script Date: 10/10/2023 6:00:00 PM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[purchase_record_line_item]
(
	[orderid] [varchar](100) NULL,
	[retail_transaction_line_itemid] [varchar](100) NULL,
	[revenue_tax_exc_local] [float] NULL,
	[revenue_tax_inc_local] [float] NULL,
	[revenue_tax_exc_AUD] [float] NULL,
	[revenue_tax_inc_AUD] [float] NULL,
	[tax_amount] [float] NULL,
	[tax_amount_AUD] [float] NULL,
	[sales_units] [int] NULL,
	[discounted_price] [float] NULL,
	[product_code] [varchar](100) NULL,
	[product_variant_type] [varchar](100) NULL,
	[source_system] [varchar](10) NULL,
	[ingestion_timestamp] [datetime] NULL,
	[create_date_purchase] [datetimeoffset](7) NULL,
	[return_flag] [char](2) NULL,
	[return_qty] [int] NULL,
	[return_value] [float] NULL,
	[return_shipping_flag] [char](2) NULL,
	[return_shipping_value] [float] NULL,
	[return_date] [datetimeoffset](7) NULL,
	[cancelled_flag] [char](2) NULL,
	[cancellation_qty] [int] NULL,
	[cancellation_value] [float] NULL,
	[cancellation_shipping_flag] [char](2) NULL,
	[cancellation_shipping_value] [float] NULL,
	[cancellation_date] [datetimeoffset](7) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[sample_flag] [char](2) NULL,
	[promotion_code] [varchar](100) NULL,
	[return_value_tax] [float] NULL,
	[cancellation_value_tax] [float] NULL,
	[discount_type] [varchar](100) NULL,
	[discount_percentage] [float] NULL,
	[orig_line_value_pre_discounts] [float] NULL,
	[bundle_sku_code] [varchar](100) NULL,
	[bundle_sku_line_no] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [retail_transaction_line_itemid] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO
