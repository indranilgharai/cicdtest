/****** Object:  Table [cons_customer].[sales_detail_time_temp]    Script Date: 12/19/2022 10:48:09 AM ******/
/****** Modified: Added 'bundle_sku_line_no' and 'bundle_sku_code'    Script Date: 10/10/2023 6:00:00 PM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_customer].[sales_detail_time_temp]
(
	[order_id] [varchar](100) NULL,
	[source_system_order_id] [varchar](100) NULL,
	[transaction_line_item_id] [varchar](100) NULL,
	[bundle_sku_line_no] [varchar](100) NULL,
	[revenue_tax_exc_local] [float] NULL,
	[revenue_tax_inc_local] [float] NULL,
	[revenue_tax_exc_AUD] [float] NULL,
	[revenue_tax_inc_AUD] [float] NULL,
	[tax_amount] [float] NULL,
	[tax_amount_AUD] [float] NULL,
	[customer_id] [varchar](100) NULL,
	[channel_type] [varchar](100) NULL,
	[customer_RFV_id] [int] NULL,
	[customer_RFV_description] [varchar](100) NULL,
	[active_subscriber] [char](2) NULL,
	[Customer_Type_Linked_Unlinked] [varchar](20) NULL,
	[product_category] [varchar](100) NULL,
	[product_sub_category] [varchar](100) NULL,
	[SKU_number] [varchar](100) NULL,
	[bundle_sku_code] [varchar](100) NULL,
	[region] [varchar](50) NULL,
	[subsidiary] [varchar](100) NULL,
	[store_address1] [varchar](100) NULL,
	[store_address2] [varchar](100) NULL,
	[store_postcode] [varchar](100) NULL,
	[store_fulfillment] [varchar](10) NULL,
	[fulfillment_store_name] [varchar](100) NULL,
	[store_name] [varchar](100) NULL,
	[origin_store] [varchar](10) NULL,
	[origin_store_name] [varchar](100) NULL,
	[subsidiary_no] [varchar](10) NULL,
	[order_total_local] [float] NULL,
	[order_total_aud] [float] NULL,
	[sales_units] [int] NULL,
	[receipt_date] [datetimeoffset](7) NULL,
	[shipped_date] [datetimeoffset](7) NULL,
	[currency_code] [varchar](10) NULL,
	[currency_type] [varchar](50) NULL,
	[discounted_price] [float] NULL,
	[staff_sale] [char](2) NULL,
	[product_replenishment] [char](2) NULL,
	[new_to_aesop] [char](2) NULL,
	[transaction_date_time_local] [datetime] NULL,
	[transaction_date_local] [date] NULL,
	[transaction_time_local] [int] NULL,
	[transaction_day_local] [int] NULL,
	[standardised_time_stamp] [datetime] NULL,
	[customer_new_to_category] [char](2) NULL,
	[sample_flag] [char](2) NULL,
	[Gift_Flag] [char](2) NULL,
	[customer_channel_type] [varchar](50) NULL,
	[customer_discount_group] [varchar](100) NULL,
	[order_type] [varchar](100) NULL,
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
	[promotion_code] [varchar](100) NULL,
	[return_value_tax] [float] NULL,
	[cancellation_value_tax] [float] NULL,
	[consultant_id] [varchar](50) NULL
)

WITH
(

	DISTRIBUTION = hash(transaction_line_item_id),
	clustered columnstore index

)
GO