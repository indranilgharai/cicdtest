CREATE PROC std.sp_stage_truncate as 
BEGIN 
truncate TABLE [stage].[cegid_order_detail]
truncate TABLE [stage].[cegid_order_header]
truncate TABLE [stage].[cegid_order_payments]
truncate TABLE [stage].[exchange_rate_x]
truncate TABLE [stage].[fps_alias]
truncate TABLE [stage].[fps_person]
truncate TABLE [stage].[hybris_cancelled_order_item]
truncate TABLE [stage].[hybris_order_details]
truncate TABLE [stage].[hybris_order_header]
truncate TABLE [stage].[hybris_returned_order_item]
truncate TABLE [stage].[hybris_order_payments]
truncate TABLE [stage].[product_x]
truncate TABLE [stage].[store_x]
truncate TABLE [stage].[subsidiary_x]

END
