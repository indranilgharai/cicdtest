SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[purchase_record_del]
(
	[orderid] [varchar](100) NULL,
	[customer_id] [varchar](100) NULL,
	[source_system_customer_id] [varchar](100) NULL,
	[channel_id] [varchar](100) NULL,
	[market_id] [varchar](100) NULL,
	[store_id] [varchar](10) NULL,
	[subsidiary_id] [varchar](10) NULL,
	[purchase_record_id] [varchar](100) NULL,
	[price] [float] NULL,
	[price_local] [float] NULL,
	[ex_rate] [float] NULL,
	[create_date_purchase] [datetimeoffset](7) NULL,
	[store_name] [varchar](200) NULL,
	[unit_count] [int] NULL,
	[ingestion_timestamp] [datetime] NULL,
	[source_system] [varchar](20) NULL,
	[orderStatus] [varchar](50) NULL,
	[currency_code] [varchar](10) NULL,
	[fulfillment_location_code] [varchar](10) NULL,
	[is_gift_card_order] [varchar](10) NULL,
	[location_code] [varchar](10) NULL,
	[storx_sbs_no] [varchar](10) NULL,
	[consignment_status_date] [datetimeoffset](7) NULL,
	[orig_product_discounts_value] [float] NULL,
	[orig_order_discounts_value] [float] NULL,
	[orig_total_discounts_value] [float] NULL,
	--Updated value from varchar(100) to varchar(500)
	[exchange_reference_id_hyrbis] [varchar](500) NULL,
	[discount_coupon_code] [varchar](100) NULL,
	[promotion_code] [varchar](1000) NULL,
	[order_shipping_total] [float] NULL,
	[order_shipping_total_tax] [float] NULL,
	[order_type] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[shipped_date] [datetimeoffset](7) NULL,
	[sales_consultant] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [orderid] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO