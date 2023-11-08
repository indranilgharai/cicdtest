/****** Modified: Added 'bundle_sku_line_no' and 'bundle_sku_code'    Script Date: 10/10/2023 6:00:00 PM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[cegid_transactions_store_sales_cegid_temp]
( 
	[header_correlationId] [nvarchar](max)  NULL,
	[header_messageType] [nvarchar](max)  NULL,
	[header_method] [nvarchar](max)  NULL,
	[header_sourceSystem] [nvarchar](max)  NULL,
	[body_filename] [nvarchar](max)  NULL,
	[document_date] [nvarchar](max)  NULL,
	[document_type] [nvarchar](max)  NULL,
	[document_store_code] [nvarchar](max)  NULL,
	[document_storewarehouse_code] [nvarchar](max)  NULL,
	[document_number] [nvarchar](max)  NULL,
	[document_internal_reference] [nvarchar](max)  NULL,
	[consultant_code] [nvarchar](max)  NULL,
	[document_currency_code] [nvarchar](max)  NULL,
	[customer_y2_code] [nvarchar](max)  NULL,
	[customer_fps_code] [nvarchar](max)  NULL,
	[customer_group_code] [nvarchar](max)  NULL,
	[document_total_amt_excl_tax] [nvarchar](max)  NULL,
	[document_total_amt_inc_tax] [nvarchar](max)  NULL,
	[document_line_number] [nvarchar](max)  NULL,
	[line_reason_code] [nvarchar](max)  NULL,
	[item_code] [nvarchar](max)  NULL,
	[line_qty] [nvarchar](max)  NULL,
	[tax_inc_unit_retail_price] [nvarchar](max)  NULL,
	[line_total_gross_amount_excl_tax] [nvarchar](max)  NULL,
	[line_total_gross_amount_incl_tax] [nvarchar](max)  NULL,
	[line_total_net_amount_excl_tax] [nvarchar](max)  NULL,
	[line_tax_model_code] [nvarchar](max)  NULL,
	[line_tax_1_system_code] [nvarchar](max)  NULL,
	[line_tax_1_total_amount] [nvarchar](max)  NULL,
	[line_tax_2_system_code] [nvarchar](max)  NULL,
	[line_tax_2_total_amount] [nvarchar](max)  NULL,
	[line_manual_discount_reason_code] [nvarchar](max)  NULL,
	[line_manual_discount_percentage] [nvarchar](max)  NULL,
	[line_manual_discount_amount] [nvarchar](max)  NULL,
	[line_sales_condition_discount_reason_code] [nvarchar](max)  NULL,
	[line_sales_condition_discount_percentage] [nvarchar](max)  NULL,
	[line_sales_condition_discount_amount] [nvarchar](max)  NULL,
	[total_line_discount_amount] [nvarchar](max)  NULL,
	[document_time] [nvarchar](max)  NULL,
	[cashier_code] [nvarchar](max)  NULL,
	[bundle_sku_code] [nvarchar](max)  NULL,
	[bundle_sku_line_no] [nvarchar](max)  NULL,
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