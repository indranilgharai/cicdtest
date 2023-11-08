/****** Object:  StoredProcedure [stage].[sp_purchase_record_union_sources]    Script Date: 6/17/2022 11:51:17 AM ******/
/**** Modified  StoredProcedure  [Added logic to keep shipped_date for shipped order status]  Modified Date: 01/07/2022 4:40:21 PM ******/
/**** Modified  StoredProcedure  [Updated deduplication logic for stage.hybris_order_header]  Modified Date: 11/05/2022 11:00:00 AM  Modified by: Patrick Lacerna ******/
/**** Modified  StoredProcedure  [Updated is_gift_card_order column logic for stage.hybris_order_header]  Modified Date: 12/09/2023 01:00:00 PM  Modified by: Gnana Prakash ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [stage].[sp_purchase_record_union_sources] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			With hybris_order_header as(
			select  distinct *,
			rank() OVER (PARTITION BY hybris_order_id ORDER BY [md_record_ingestion_timestamp] desc,last_modified_date desc,header_method desc,header_correlationid desc) AS dupcnt 				
			from [stage].[hybris_order_header]
			)
			--updated the code to handle shipped_date value. 

			insert into stage.purchase_record_union_sources
			select 
			a.purchase_id,
                        a.source_system_order_id,
                        a.customer_id,
			a.source_system_customer_id,
			a.subsidiary_number,
			a.store_number,
			a.order_total ,
			a.order_total_tax ,
			a.order_shipping_total,
			a.order_shipping_total_tax,
			a.order_discount ,
			a.currency ,
			a.status ,
			a.create_date_purchase ,
			a.is_gift_card_order ,
			a.order_type ,
			a.fulfillment_location_code ,
			a.total_items_unit_count ,
			a.source_system ,
			a.ingestion_timestamp,
			a.location_code,
			a.consignment_status_date,
			a.orig_product_discounts_value,
			a.orig_order_discounts_value,
			a.orig_total_discounts_value,
			a.exchange_reference_id_hyrbis,
			a.discount_coupon_code,
			a.promotion_code,
			a.md_record_written_timestamp,
			a.md_record_written_pipeline_id,
			a.md_transformation_job_id,
			a.md_source_system,
			hso.shipped_date 
			from 
			(
				select distinct
				'C'+cegid_order_id as purchase_id ,
				cegid_order_id as source_system_order_id ,
				user_id as customer_id ,
				user_id as source_system_customer_id ,
				sbs_no as subsidiary_number ,
				store_no as store_number ,
				isnull(cast(total_price_value as float),0) as order_total ,
				isnull(cast(total_price_with_tax_value as float),0) as order_total_tax ,
				null as order_shipping_total,
				null as order_shipping_total_tax,
				null as order_discount ,
				currency_iso as currency ,
				order_status as status ,
				coalesce(created_time,created) as create_date_purchase ,
				null as is_gift_card_order ,
				null as order_type ,
				null as fulfillment_location_code ,
				total_items as total_items_unit_count ,
				'CEGID' as source_system ,
				CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as ingestion_timestamp,
				FORMAT(CAST(sbs_no AS INT),'00','en-US')+FORMAT(CAST(store_no AS INT),'000','en-US') as location_code,
				coalesce(created_time,created) as consignment_status_date,
				null as orig_product_discounts_value,
				null as orig_order_discounts_value,
				null as orig_total_discounts_value,
				null as exchange_reference_id_hyrbis,
				null as discount_coupon_code,
				null as promotion_code,
				getDate() as md_record_written_timestamp,
				@pipelineid as md_record_written_pipeline_id,
				@jobid as md_transformation_job_id,
				md_source_system as md_source_system
				from [stage].[cegid_order_header]
			
			union 
			
				select distinct 
				'H'+hybris_order_id		as	purchase_id	,   
				hybris_order_id	as	source_system_order_id	,
				user_fps_id	as	customer_id	,
				user_hybris_id	as	source_system_customer_id	,
				subsidiary_number	as	subsidiary_number	,
				store_number	as	store_number	,
				isnull(cast((total_price_value) as float),0)	as	order_total	,
				isnull(cast(total_price_with_tax_value as float),0) as	order_total_tax	,
				isnull(cast(delivery_cost_value as float),0) as order_shipping_total,
				isnull(cast(delivery_cost_tax_rate as float),0) as order_shipping_total_tax,
				isnull(cast(total_discounts_value as float),0)	as	order_discount	,
				total_price_with_tax_currency_iso	as	currrency	,
				status	as	status	,
				created	as	create_date_purchase	,
				CASE WHEN is_gift_card_order=1 or is_gift_wrap=1 THEN 'Y' ELSE 'N' END as   is_gift_card_order  ,
				order_type	as	order_type	,
				fulfillment_location_code	as	fulfillment_location_code	,
				delivery_items_quantity	as	total_items_unit_count	,
				'HYBRIS'	as	source_system,
				CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as ingestion_timestamp,
				coalesce(store_code,FORMAT(CAST(subsidiary_number AS INT),'00','en-US')+FORMAT(CAST(store_number AS INT),'000','en-US')) as location_code,
				last_modified_date as consignment_status_date,
				isnull(cast(orig_product_discounts_value as float),0) as orig_product_discounts_value,
				isnull(cast(orig_order_discounts_value as float),0) as orig_order_discounts_value,
				isnull(cast(orig_total_discounts_value as float),0) as orig_total_discounts_value,
				exchange_ref_id as exchange_reference_id_hyrbis,
				applied_product_used_coupons as discount_coupon_code,
				applied_product_promotion_code as promotion_code,
				getDate() as md_record_written_timestamp,
				@pipelineid as md_record_written_pipeline_id,
				@jobid as md_transformation_job_id,
				md_source_system as md_source_system
			from (select * from hybris_order_header where dupcnt=1) hoh	
			)a
			--updated the code to handle shipped_date value. 
			LEFT JOIN std.hybris_shipped_orders hso on hso.order_id = a.purchase_id
			
			where source_system_order_id is not null
			OPTION (LABEL = 'AADPSTGPURREC');

			UPDATE STATISTICS stage.hybris_order_header;
			UPDATE STATISTICS [stage].[cegid_order_header];
            UPDATE STATISTICS stage.purchase_record_union_sources;

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTGLINEITM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			TRUNCATE TABLE [stage].[hybris_order_header];
			TRUNCATE TABLE [stage].[cegid_order_header];	
		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from stage.purchase_record_union_sources;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from stage.purchase_record_union_sources where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'stage.sp_purchase_record_union_sources' AS ErrorProcedure ,
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
END
