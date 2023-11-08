/****** Modified: Modified merge logic   Script Date: 8/21/2023 10:30:00 AM   Modified By: Patrick Lacerna ******/
/****** Modified: Modified date conversions   Script Date: 9/05/2023 10:30:00 AM   Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_purchase_record] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			IF OBJECT_ID('tempdb..#purchase_record') IS NOT NULL
			BEGIN
				DROP TABLE #purchase_record
			END
			CREATE TABLE #purchase_record
			WITH
            (
				DISTRIBUTION = HASH ( [orderid] ),
				CLUSTERED COLUMNSTORE INDEX
            ) AS 

			WITH pr_base as (
                SELECT  
                DISTINCT  purchase_id AS [orderid] 
                ,coalesce(pr.customer_id,pr.source_system_customer_id) AS [customer_id]
                ,pr.source_system_customer_id as source_system_customer_id
                /*-----------------------channed_id for Cegid is coming from store_x ref table--------*/
                ,isnull(st.channel,'') AS [channel_id]
                ,sbs_name AS [market_id]
                ,pr.store_number  AS [store_id]
                ,pr.subsidiary_number   AS [subsidiary_id]
                ,pr.purchase_id AS [purchase_record_id] 
                /*------------currency conversion-----------*/
                ,case when currency='AUD' then pr.order_total_tax else (pr.order_total_tax/exrate.ex_rate) end as price
                ,pr.order_total_tax AS price_local
                ,exrate.ex_rate as ex_rate
                ,coalesce(try_convert(datetimeoffset,NULLIF(create_date_purchase, '')),try_convert(datetimeoffset,concat(substring(create_date_purchase,1,19),substring(create_date_purchase,20,3),':',substring(create_date_purchase,23,2)))) as create_date_purchase
                ,st.store_name AS [store_name]
                ,pr.total_items_unit_count AS [unit_count] 
                ,pr.ingestion_timestamp as ingestion_timestamp
                ,pr.source_system 
                ,pr.status as[orderStatus]
                ,pr.[currency] as currency_code
                ,pr.[fulfillment_location_code] as [fulfillment_location_code]
                ,is_gift_card_order as is_gift_card_order
                ,pr.location_code
                ,st.sbs_no as  storx_sbs_no
                ,coalesce(try_convert(datetimeoffset,NULLIF(consignment_status_date, '')),try_convert(datetimeoffset,concat(substring(consignment_status_date,1,19),substring(consignment_status_date,20,3),':',substring(consignment_status_date,23,2)))) as consignment_status_date
                ,orig_product_discounts_value as orig_product_discounts_value
                ,orig_order_discounts_value as orig_order_discounts_value
                ,orig_total_discounts_value as orig_total_discounts_value
                ,exchange_reference_id_hyrbis as exchange_reference_id_hyrbis
                ,discount_coupon_code as discount_coupon_code
                ,promotion_code as promotion_code
                ,order_shipping_total as order_shipping_total
                ,order_shipping_total_tax as order_shipping_total_tax
                ,order_type	as	order_type	
                ------------------------metadata fields-------------------
                ,getDate() as md_record_written_timestamp
                ,@pipelineid	   as md_record_written_pipeline_id
                ,@jobid 	   as md_transformation_job_id	
                ,'DERIVED' as md_source_system
                ,coalesce(try_convert(datetimeoffset,NULLIF(shipped_date, '')),try_convert(datetimeoffset,concat(substring(shipped_date,1,19),substring(shipped_date,20,3),':',substring(shipped_date,23,2)))) as shipped_date
                ,null as sales_consultant
                FROM stage.purchase_record_union_sources pr
                left JOIN std.store_x st on cast(pr.location_code as int)=cast(st.location_code as int)
                left JOIN std.subsidiary_x sub ON cast(st.sbs_no as int) = cast(sub.sbs_no as int)
                LEFT JOIN std.exchange_rate_x exrate ON 
                trim(coalesce(cast(st.sbs_no AS VARCHAR),pr.subsidiary_number)) = trim(cast(exrate.sbs_no AS VARCHAR))
                AND cast(exrate.year AS INT) = 
                    cast(year(coalesce(try_convert(datetimeoffset,NULLIF(create_date_purchase, '')),try_convert(datetimeoffset,concat(substring(create_date_purchase,1,19),substring(create_date_purchase,20,3),':',substring(create_date_purchase,23,2))))) AS INT)
                AND cast(exrate.month_no AS INT) = 
                    cast(month(coalesce(try_convert(datetimeoffset,NULLIF(create_date_purchase, '')),try_convert(datetimeoffset,concat(substring(create_date_purchase,1,19),substring(create_date_purchase,20,3),':',substring(create_date_purchase,23,2))))) AS INT)
            )
            
            select distinct 
				[orderid] ,
				[customer_id],
				[source_system_customer_id],
				[channel_id] ,
				[market_id] ,
				[store_id],
				[subsidiary_id] ,
				[purchase_record_id] ,
				[price],
				[price_local],
				[ex_rate],
				[create_date_purchase],
				[store_name],
				[unit_count],
				[ingestion_timestamp] ,
				[source_system] ,
				[orderStatus] ,
				[currency_code] ,
				[fulfillment_location_code],
				[is_gift_card_order],
				[location_code],
				[storx_sbs_no],
				[consignment_status_date],
				[orig_product_discounts_value],
				[orig_order_discounts_value],
				[orig_total_discounts_value],
				[exchange_reference_id_hyrbis],
				[discount_coupon_code],
				string_agg([promotion_code],';') as [promotion_code],
				[order_shipping_total],
				[order_shipping_total_tax],
				[order_type],
				[md_record_written_timestamp],
				[md_record_written_pipeline_id],
				[md_transformation_job_id],
				[md_source_system],
				[shipped_date],
				sales_consultant
			from (
				SELECT *,
				rank() OVER (PARTITION BY orderid ORDER BY consignment_status_date desc, ingestion_timestamp desc, md_record_written_timestamp desc) AS dupcnt
				FROM pr_base
			)a WHERE dupcnt=1 
				group by [orderid] ,
				[customer_id],
				[source_system_customer_id],
				[channel_id] ,
				[market_id] ,
				[store_id],
				[subsidiary_id] ,
				[purchase_record_id] ,
				[price],
				[price_local],
				[ex_rate],
				[create_date_purchase],
				[store_name],
				[unit_count],
				[ingestion_timestamp] ,
				[source_system] ,
				[orderStatus] ,
				[currency_code] ,
				[fulfillment_location_code],
				[is_gift_card_order],
				[location_code],
				[storx_sbs_no],
				[consignment_status_date],
				[orig_product_discounts_value],
				[orig_order_discounts_value],
				[orig_total_discounts_value],
				[exchange_reference_id_hyrbis],
				[discount_coupon_code],
				[order_shipping_total],
				[order_shipping_total_tax],
				[order_type],
				[md_record_written_timestamp],
				[md_record_written_pipeline_id],
				[md_transformation_job_id],
				[md_source_system],
				[shipped_date],
				sales_consultant;

			MERGE [std].[purchase_record] AS TargetTbl
			USING #purchase_record AS SourceTbl
			ON SourceTbl.orderid = TargetTbl.orderid

			-- Insert streaming data if record does not exist yet
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (
					[orderid] ,
                    [customer_id] ,
                    [source_system_customer_id] ,
                    [channel_id] ,
                    [market_id] ,
                    [store_id] ,
                    [subsidiary_id] ,
                    [purchase_record_id] ,
                    [price] ,
                    [price_local] ,
                    [ex_rate] ,
                    [create_date_purchase] ,
                    [store_name] ,
                    [unit_count] ,
                    [ingestion_timestamp] ,
                    [source_system] ,
                    [orderStatus] ,
                    [currency_code] ,
                    [fulfillment_location_code] ,
                    [is_gift_card_order] ,
                    [location_code] ,
                    [storx_sbs_no] ,
                    [consignment_status_date] ,
                    [orig_product_discounts_value] ,
                    [orig_order_discounts_value] ,
                    [orig_total_discounts_value] ,
                    [exchange_reference_id_hyrbis] ,
                    [discount_coupon_code] ,
                    [promotion_code] ,
                    [order_shipping_total] ,
                    [order_shipping_total_tax] ,
                    [order_type] ,
                    [md_record_written_timestamp] ,
                    [md_record_written_pipeline_id] ,
                    [md_transformation_job_id] ,
                    [md_source_system] ,
                    [shipped_date] ,
                    [sales_consultant] 
					) 
					VALUES (
					[SourceTbl].[orderid] ,
                    [SourceTbl].[customer_id] ,
                    [SourceTbl].[source_system_customer_id] ,
                    [SourceTbl].[channel_id] ,
                    [SourceTbl].[market_id] ,
                    [SourceTbl].[store_id] ,
                    [SourceTbl].[subsidiary_id] ,
                    [SourceTbl].[purchase_record_id] ,
                    [SourceTbl].[price] ,
                    [SourceTbl].[price_local] ,
                    [SourceTbl].[ex_rate] ,
                    [SourceTbl].[create_date_purchase] ,
                    [SourceTbl].[store_name] ,
                    [SourceTbl].[unit_count] ,
                    [SourceTbl].[ingestion_timestamp] ,
                    [SourceTbl].[source_system] ,
                    [SourceTbl].[orderStatus] ,
                    [SourceTbl].[currency_code] ,
                    [SourceTbl].[fulfillment_location_code] ,
                    [SourceTbl].[is_gift_card_order] ,
                    [SourceTbl].[location_code] ,
                    [SourceTbl].[storx_sbs_no] ,
                    [SourceTbl].[consignment_status_date] ,
                    [SourceTbl].[orig_product_discounts_value] ,
                    [SourceTbl].[orig_order_discounts_value] ,
                    [SourceTbl].[orig_total_discounts_value] ,
                    [SourceTbl].[exchange_reference_id_hyrbis] ,
                    [SourceTbl].[discount_coupon_code] ,
                    [SourceTbl].[promotion_code] ,
                    [SourceTbl].[order_shipping_total] ,
                    [SourceTbl].[order_shipping_total_tax] ,
                    [SourceTbl].[order_type] ,
                    [SourceTbl].[md_record_written_timestamp] ,
                    [SourceTbl].[md_record_written_pipeline_id] ,
                    [SourceTbl].[md_transformation_job_id] ,
                    [SourceTbl].[md_source_system] ,
                    [SourceTbl].[shipped_date] ,
                    [SourceTbl].[sales_consultant] 
					)
            -- Does not update std.purchase_record if CEGID EOD data already present
			WHEN MATCHED AND [TargetTbl].[md_source_system] <> 'CEGID'  THEN
				UPDATE SET 
					[orderid] = [SourceTbl].[orderid],
                    [customer_id] = [SourceTbl].[customer_id],
                    [source_system_customer_id] = [SourceTbl].[source_system_customer_id],
                    [channel_id] = [SourceTbl].[channel_id],
                    [market_id] = [SourceTbl].[market_id],
                    [store_id] = [SourceTbl].[store_id],
                    [subsidiary_id] = [SourceTbl].[subsidiary_id],
                    [purchase_record_id] = [SourceTbl].[purchase_record_id],
                    [price] = [SourceTbl].[price],
                    [price_local] = [SourceTbl].[price_local],
                    [ex_rate] = [SourceTbl].[ex_rate],
                    [create_date_purchase] = [SourceTbl].[create_date_purchase],
                    [store_name] = [SourceTbl].[store_name],
                    [unit_count] = [SourceTbl].[unit_count],
                    [ingestion_timestamp] = [SourceTbl].[ingestion_timestamp],
                    [source_system] = [SourceTbl].[source_system],
                    [orderStatus] = [SourceTbl].[orderStatus],
                    [currency_code] = [SourceTbl].[currency_code],
                    [fulfillment_location_code] = [SourceTbl].[fulfillment_location_code],
                    [is_gift_card_order] = [SourceTbl].[is_gift_card_order],
                    [location_code] = [SourceTbl].[location_code],
                    [storx_sbs_no] = [SourceTbl].[storx_sbs_no],
                    [consignment_status_date] = [SourceTbl].[consignment_status_date],
                    [orig_product_discounts_value] = [SourceTbl].[orig_product_discounts_value],
                    [orig_order_discounts_value] = [SourceTbl].[orig_order_discounts_value],
                    [orig_total_discounts_value] = [SourceTbl].[orig_total_discounts_value],
                    [exchange_reference_id_hyrbis] = [SourceTbl].[exchange_reference_id_hyrbis],
                    [discount_coupon_code] = [SourceTbl].[discount_coupon_code],
                    [promotion_code] = [SourceTbl].[promotion_code],
                    [order_shipping_total] = [SourceTbl].[order_shipping_total],
                    [order_shipping_total_tax] = [SourceTbl].[order_shipping_total_tax],
                    [order_type] = [SourceTbl].[order_type],
                    [md_record_written_timestamp] = [SourceTbl].[md_record_written_timestamp],
                    [md_record_written_pipeline_id] = [SourceTbl].[md_record_written_pipeline_id],
                    [md_transformation_job_id] = [SourceTbl].[md_transformation_job_id],
                    [md_source_system] = [SourceTbl].[md_source_system],
                    [shipped_date] = [SourceTbl].[shipped_date],
                    [sales_consultant]  = [SourceTbl].[sales_consultant]
			OPTION (LABEL = 'AADSTDPURREC');

			UPDATE STATISTICS [std].[purchase_record];

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDPURREC'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			--History insertion is done in purchase_record_cegid_eod
			--Insert into std.purchase_record_history
			--select * from std.purchase_record;

			TRUNCATE TABLE stage.purchase_record_union_sources;		
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.purchase_record;
			
			delete from std.purchase_record where md_record_written_timestamp=@newrec;
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_purchase_record' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END
