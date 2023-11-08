/****** Object:  Table [stage].[hybris_order_header]    Script Date: 3/22/2022 7:01:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[hybris_order_header]
(
	[header_correlationId] [nvarchar](max) NULL,
	[header_messageType] [nvarchar](max) NULL,
	[header_method] [nvarchar](max) NULL,
	[header_sourceSystem] [nvarchar](max) NULL,
	[created] [nvarchar](max) NULL,
	[status] [nvarchar](max) NULL,
	[customer_type] [nvarchar](max) NULL,
	[hybris_order_id] [nvarchar](max) NULL,
	[erp_order_id] [nvarchar](max) NULL,
	[total_price_with_tax_currency_iso] [nvarchar](max) NULL,
	[orig_total_price_with_tax_value] [decimal](38, 18) NULL,
	[total_price_with_tax_value] [decimal](38, 18) NULL,
	[orig_total_price_value] [decimal](38, 18) NULL,
	[total_price_value] [decimal](38, 18) NULL,
	[orig_total_tax_value] [decimal](38, 18) NULL,
	[total_tax_value] [decimal](38, 18) NULL,
	[orig_sub_total_value] [decimal](38, 18) NULL,
	[sub_total_value] [decimal](38, 18) NULL,
	[orig_delivery_cost_value] [decimal](38, 18) NULL,
	[delivery_cost_value] [decimal](38, 18) NULL,
	[orig_delivery_cost_tax_rate] [nvarchar](max) NULL,
	[delivery_cost_tax_rate] [nvarchar](max) NULL,
	[total_items] [int] NULL,
	[delivery_mode_code] [nvarchar](max) NULL,
	[delivery_mode_name] [nvarchar](max) NULL,
	[delivery_mode_delivery_cost_value] [decimal](38, 18) NULL,
	[delivery_address_full_name] [nvarchar](max) NULL,
	[delivery_address_postal_code] [nvarchar](max) NULL,
	[delivery_address_country_iso_code] [nvarchar](max) NULL,
	[delivery_address_country_name] [nvarchar](max) NULL,
	[delivery_address_town] [nvarchar](max) NULL,
	[orig_product_discounts_value] [decimal](38, 18) NULL,
	[product_discounts_value] [decimal](38, 18) NULL,
	[orig_order_discounts_value] [decimal](38, 18) NULL,
	[order_discounts_value] [decimal](38, 18) NULL,
	[orig_total_discounts_value] [decimal](38, 18) NULL,
	[total_discounts_value] [decimal](38, 18) NULL,
	[site] [nvarchar](max) NULL,
	[guid] [nvarchar](max) NULL,
	[calculated] [int] NULL,
	[hsh_id] [nvarchar](max) NULL,
	[user_name] [nvarchar](max) NULL,
	[user_hybris_id] [nvarchar](max) NULL,
	[user_erp_id] [nvarchar](max) NULL,
	[orig_delivery_items_quantity] [int] NULL,
	[delivery_items_quantity] [int] NULL,
	[is_gift_card_order] [bit] NULL,
	[is_gift_wrap] [bit] NULL,
	[gift_message] [nvarchar](max) NULL,
	[shipped_date] [nvarchar](max) NULL,
	[exchange_ref_id] [nvarchar](max) NULL,
	[comment] [nvarchar](max) NULL,
	[user_fps_id] [nvarchar](max) NULL,
	[store_code] [nvarchar](max) NULL,
	[fulfillment_location_code] [nvarchar](max) NULL,
	[order_type] [nvarchar](max) NULL,
	[bundle_name] [nvarchar](max) NULL,
	[order_origin_store_code] [nvarchar](max) NULL,
	[applied_product_used_coupons] [nvarchar](max) NULL,
	[applied_product_promotion_code] [nvarchar](max) NULL,
	[applied_order_used_coupons] [nvarchar](max) NULL,
	[applied_order_promotion_code] [nvarchar](max) NULL,
	[last_modified_date] [nvarchar](max) NULL,
	[store_number] [nvarchar](max) NULL,
	[subsidiary_number] [nvarchar](max) NULL,
	[isCncOrder] [bit] NULL,
	[virtualWarehouseCodeLocationCode] [nvarchar](max) NULL,
	[cncStoreId] [nvarchar](max) NULL,
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