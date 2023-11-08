SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[line_item_union_sources]
(
	[line_item_id] [varchar](100) NULL,
	[order_id] [varchar](100) NULL,
	[entry_number] [varchar](100) NULL,
	[quantity] [int] NULL,
	[total_price_currency_iso] [varchar](100) NULL,
	[total_price_price_type] [varchar](100) NULL,
	[orig_total_price_value] [float] NULL,
	[total_price_value] [float] NULL,
	[product_code] [varchar](100) NULL,
	[product_name] [varchar](500) NULL,
	[product_url] [varchar](500) NULL,
	[product_purchasable] [char](2) NULL,
	[product_variant_type] [varchar](100) NULL,
	[tax_id] [varchar](100) NULL,
	[tax_rate] [varchar](100) NULL,
	[tax_rate_gsthst] [varchar](100) NULL,
	[tax_rate_pst] [varchar](100) NULL,
	[source_system] [varchar](10) NULL,
	[ingestion_timestamp] [datetime] NULL,
	[sbs_no] [varchar](100) NULL,
	[create_date_purchase] [varchar](100) NULL,
	[return_flag] [char](2) NULL,
	[return_qty] [int] NULL,
	[return_value] [float] NULL,
	[return_shipping_flag] [char](2) NULL,
	[return_shipping_value] [float] NULL,
	[return_date] [varchar](50) NULL,
	[cancelled_flag] [char](2) NULL,
	[cancellation_qty] [int] NULL,
	[cancellation_value] [float] NULL,
	[cancellation_shipping_flag] [char](2) NULL,
	[cancellation_shipping_value] [float] NULL,
	[cancellation_date] [varchar](50) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[promotion_code] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO