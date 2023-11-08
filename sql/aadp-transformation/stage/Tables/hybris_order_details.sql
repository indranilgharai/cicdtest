/****** Object:  Table [stage].[hybris_order_details]    Script Date: 3/22/2022 7:01:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[hybris_order_details]
(
	[header_correlationId] [nvarchar](max) NULL,
	[header_messageType] [nvarchar](max) NULL,
	[header_method] [nvarchar](max) NULL,
	[header_sourceSystem] [nvarchar](max) NULL,
	[body_hybrisOrderId] [nvarchar](max) NULL,
	[entry_number] [int] NULL,
	[quantity] [int] NULL,
	[quantity_pending] [int] NULL,
	[quantity_shipped] [int] NULL,
	[total_price_currency_iso] [nvarchar](max) NULL,
	[total_price_price_type] [nvarchar](max) NULL,
	[orig_total_price_value] [decimal](38, 18) NULL,
	[total_price_value] [decimal](38, 18) NULL,
	[product_code] [nvarchar](max) NULL,
	[product_name] [nvarchar](max) NULL,
	[product_url] [nvarchar](max) NULL,
	[product_purchasable] [bit] NULL,
	[product_variant_type] [nvarchar](max) NULL,
	[tax_id] [nvarchar](max) NULL,
	[tax_rate] [decimal](38, 18) NULL,
	[tax_rate_gsthst] [decimal](38, 18) NULL,
	[tax_rate_pst] [decimal](38, 18) NULL,
	[promotions_desc] [nvarchar](max) NULL,
	[discounted_unit_price_ex_tax] [decimal](38, 18) NULL,
	[promotion_code] [nvarchar](max) NULL,
	[promotion_type] [nvarchar](max) NULL,
	[used_coupons] [nvarchar](max) NULL,
	[created] [nvarchar](max) NULL,
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