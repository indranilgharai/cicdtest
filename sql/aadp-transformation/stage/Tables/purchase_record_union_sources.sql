SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[purchase_record_union_sources]
(
	[purchase_id] [varchar](100) NULL,
	[source_system_order_id] [varchar](100) NULL,
	[customer_id] [varchar](100) NULL,
	[source_system_customer_id] [varchar](100) NULL,
	[subsidiary_number] [varchar](10) NULL,
	[store_number] [varchar](10) NULL,
	[order_total] [float] NULL,
	[order_total_tax] [float] NULL,
	[order_shipping_total] [float] NULL,
	[order_shipping_total_tax] [float] NULL,
	[order_discount] [float] NULL,
	[currency] [varchar](10) NULL,
	[status] [varchar](50) NULL,
	[create_date_purchase] [varchar](100) NULL,
	[is_gift_card_order] [varchar](10) NULL,
	[order_type] [varchar](100) NULL,
	[fulfillment_location_code] [varchar](10) NULL,
	[total_items_unit_count] [int] NULL,
	[source_system] [varchar](10) NULL,
	[ingestion_timestamp] [datetime] NULL,
	[location_code] [varchar](10) NULL,
	[consignment_status_date] [varchar](100) NULL,
	[orig_product_discounts_value] [float] NULL,
	[orig_order_discounts_value] [float] NULL,
	[orig_total_discounts_value] [float] NULL,
	--Updated the value from varchar(100) to varchar(500)
	[exchange_reference_id_hyrbis] [varchar](500) NULL,
	[discount_coupon_code] [varchar](100) NULL,
	[promotion_code] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[shipped_date] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO