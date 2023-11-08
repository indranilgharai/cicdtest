/****** Modified: Modified merge logic   Script Date: 8/21/2023 10:30:00 AM   Modified By: Patrick Lacerna ******/
/****** Modified: Modified date conversions   Script Date: 9/05/2023 10:30:00 AM   Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_purchase_record_line_item] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			IF OBJECT_ID('tempdb..#purchase_record_line_item') IS NOT NULL
			BEGIN
				DROP TABLE #purchase_record_line_item
			END
			CREATE TABLE #purchase_record_line_item
			WITH
            (
                	DISTRIBUTION = HASH ( [retail_transaction_line_itemid] ),
	                CLUSTERED COLUMNSTORE INDEX
            ) AS 

            WITH tax_amt as (
			select
			 li.[order_id] AS [orderid]
			,li.[line_item_id] AS [line_item_id]
			,pr.location_code as location_code
			,pr.subsidiary_id as subsidiary_id
			,st.sbs_no as sbs_no
			,sub.sbs_name as sbs_name
			,exrate.ex_rate as ex_rate
			,pr.create_date_purchase as create_date_purchase			
			,case when li.[source_system]='CEGID' then li.[tax_rate]
				  when (li.[source_system]='HYBRIS' and upper(coalesce(sub.sbs_name,'NULL'))='CANADA') 
					then (cast(li.[total_price_value] as float) * ((cast(li.tax_rate_gsthst as float)+cast(tax_rate_pst as float))/cast(100 as float)))
				  else (cast(li.[total_price_value] as float) * cast(li.[tax_rate]as float)/cast(100 as float)) 
				  end as [tax_amount],
			upper(prd.product_type_cat) as product_type_cat,
			upper(prd.product_type_sub_cat) as product_type_sub_cat
			FROM [stage].[line_item_union_sources] li	
			left JOIN std.purchase_record pr on pr.[orderid] = li.[order_id]
			LEFT JOIN std.product_X prd ON prd.description1 = li.product_code
			left JOIN std.store_x st on cast(pr.location_code as int)=cast(st.location_code as int)
			left JOIN std.subsidiary_x sub ON cast(st.sbs_no as int) = cast(sub.sbs_no as int)
			LEFT JOIN std.exchange_rate_x exrate ON 
			trim(coalesce(cast(st.sbs_no AS VARCHAR),pr.subsidiary_id)) = trim(cast(exrate.sbs_no AS VARCHAR))
				AND cast(exrate.year AS INT) = cast(year(pr.create_date_purchase) AS INT)
				AND cast(exrate.month_no AS INT) = cast(month(pr.create_date_purchase) AS INT)
			)
			,prli_base as (
                SELECT DISTINCT 
                li.[order_id] AS [orderid]
                ,li.[line_item_id] AS [retail_transaction_line_itemid]
                ,ISNULL(cast(li.[total_price_value] as float),0) AS [revenue_tax_exc_local]
                ,ISNULL(cast((li.[total_price_value]+ta.[tax_amount]) as float) ,0) AS [revenue_tax_inc_local]
                ,ISNULL(case when total_price_currency_iso='AUD' then cast([total_price_value]as float) else (cast([total_price_value] as float)/ta.ex_rate) end, 0) AS [revenue_tax_exc_AUD] 
                ,ISNULL(case when total_price_currency_iso='AUD' then cast((li.[total_price_value]+ta.[tax_amount]) as float) else (cast((li.[total_price_value]+ta.[tax_amount]) as float)/ta.ex_rate) end,0) AS [revenue_tax_inc_AUD]
                ,ISNULL(cast(ta.[tax_amount] as float),0) as [tax_amount]
                ,ISNULL(case when total_price_currency_iso='AUD' then cast(ta.[tax_amount] as float) else (cast(ta.[tax_amount] as float)/ta.ex_rate) end,  0) AS [tax_amount_AUD] 
                --------- sample products to be excluded from sales_units calculation--------------
                ,CASE WHEN ta.product_type_cat='FINISHED GOOD' AND (ta.product_type_sub_cat='SAMPLE' or ta.product_type_sub_cat='PREMIUM SAMPLE')
                        THEN 0 ELSE cast(quantity as int)  END  AS sales_units		
                ,case when li.source_system='CEGID' then ISNULL(cast(li.total_price_value as float),0) 
                        when li.source_system='HYBRIS' then ISNULL(cast(li.orig_total_price_value as float),0) end AS discounted_price
                ,[product_code] AS [product_code]
                ,product_variant_type as product_variant_type
                ,[source_system] AS [source_system]
                ,[ingestion_timestamp] AS [ingestion_timestamp]
                ,ta.create_date_purchase as create_date_purchase
                ,return_flag as return_flag
                ,return_qty as return_qty
                ,return_value as return_value
                ,return_shipping_flag as return_shipping_flag
                ,return_shipping_value as return_shipping_value
                ,coalesce(try_convert(datetimeoffset,NULLIF(return_date,'')),try_convert(datetimeoffset,concat(substring(return_date,1,19),substring(return_date,20,3),':',substring(return_date,23,2)))) as return_date
                ,cancelled_flag as cancelled_flag
                ,cancellation_qty as cancellation_qty
                ,cancellation_value as  cancellation_value
                ,cancellation_shipping_flag as cancellation_shipping_flag
                ,cancellation_shipping_value as cancellation_shipping_value
                ,coalesce(try_convert(datetimeoffset,NULLIF(cancellation_date,'')),try_convert(datetimeoffset,concat(substring(cancellation_date,1,19),substring(cancellation_date,20,3),':',substring(cancellation_date,23,2)))) as cancellation_date
                -----metadata fields---		
                ,getDate() as md_record_written_timestamp
                ,@pipelineid as md_record_written_pipeline_id
                ,@jobid as md_transformation_job_id
                ,'DERIVED' as md_source_system
                ----sample_flag logic implementation--------
                ,CASE WHEN ta.product_type_cat='FINISHED GOOD' AND (ta.product_type_sub_cat='SAMPLE' or ta.product_type_sub_cat='PREMIUM SAMPLE')
                        THEN 'Y' ELSE 'N' END AS sample_flag
                
                ,promotion_code as promotion_code
                ,case when [quantity]=0 then return_value else (return_value+((ta.[tax_amount]/[quantity])*return_qty))  end as return_value_tax
                ,case when [quantity]=0 then cancellation_value else (cancellation_value+((ta.[tax_amount]/[quantity])*cancellation_qty)) end as  cancellation_value_tax
                ,null as [discount_type]
                ,null as [discount_percentage]
                ,null as [orig_line_value_pre_discounts]
                FROM [stage].[line_item_union_sources] li	
                LEFT JOIN tax_amt ta on li.order_id=ta.orderid and li.[line_item_id]=ta.[line_item_id]
            )
            
            select distinct 
				[orderid],
				[retail_transaction_line_itemid],
				[revenue_tax_exc_local],
				[revenue_tax_inc_local],
				[revenue_tax_exc_AUD],
				[revenue_tax_inc_AUD],
				[tax_amount],
				[tax_amount_AUD],
				[sales_units],
				[discounted_price],
				[product_code],
				[product_variant_type],
				[source_system],
				[ingestion_timestamp],
				[create_date_purchase],
				[return_flag],
				[return_qty],
				[return_value],
				[return_shipping_flag],
				[return_shipping_value],
				[return_date],
				[cancelled_flag],
				[cancellation_qty],
				[cancellation_value],
				[cancellation_shipping_flag],
				[cancellation_shipping_value],
				[cancellation_date],		
				[md_record_written_timestamp],
				[md_record_written_pipeline_id],
				[md_transformation_job_id],
				[md_source_system],
				[sample_flag],
				string_agg([promotion_code],';') as [promotion_code],
				[return_value_tax],
				[cancellation_value_tax],
				[discount_type],
				[discount_percentage],
				[orig_line_value_pre_discounts]	
			from (
				SELECT *,rank() OVER (PARTITION BY orderid,retail_transaction_line_itemid ORDER BY ingestion_timestamp desc,create_date_purchase desc,md_record_written_timestamp desc) AS dupcnt
				FROM prli_base
			)a WHERE dupcnt=1 
				group by [orderid],
				[retail_transaction_line_itemid],
				[revenue_tax_exc_local],
				[revenue_tax_inc_local],
				[revenue_tax_exc_AUD],
				[revenue_tax_inc_AUD],
				[tax_amount],
				[tax_amount_AUD],
				[sales_units],
				[discounted_price],
				[product_code],
				[product_variant_type],
				[source_system],
				[ingestion_timestamp],
				[create_date_purchase],
				[return_flag],
				[return_qty],
				[return_value],
				[return_shipping_flag],
				[return_shipping_value],
				[return_date],
				[cancelled_flag],
				[cancellation_qty],
				[cancellation_value],
				[cancellation_shipping_flag],
				[cancellation_shipping_value],
				[cancellation_date],		
				[md_record_written_timestamp],
				[md_record_written_pipeline_id],
				[md_transformation_job_id],
				[md_source_system],
				[sample_flag],
				[return_value_tax],
				[cancellation_value_tax],
				[discount_type],
				[discount_percentage],
				[orig_line_value_pre_discounts]	;

			MERGE [std].[purchase_record_line_item] AS TargetTbl
			USING #purchase_record_line_item AS SourceTbl
			ON SourceTbl.orderid = TargetTbl.orderid
            AND SourceTbl.retail_transaction_line_itemid = TargetTbl.retail_transaction_line_itemid

			-- Insert streaming data if record does not exist yet
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (
					[orderid],
                    [retail_transaction_line_itemid],
                    [revenue_tax_exc_local],
                    [revenue_tax_inc_local],
                    [revenue_tax_exc_AUD],
                    [revenue_tax_inc_AUD],
                    [tax_amount],
                    [tax_amount_AUD],
                    [sales_units],
                    [discounted_price],
                    [product_code],
                    [product_variant_type],
                    [source_system],
                    [ingestion_timestamp],
                    [create_date_purchase],
                    [return_flag],
                    [return_qty],
                    [return_value],
                    [return_shipping_flag],
                    [return_shipping_value],
                    [return_date],
                    [cancelled_flag],
                    [cancellation_qty],
                    [cancellation_value],
                    [cancellation_shipping_flag],
                    [cancellation_shipping_value],
                    [cancellation_date],		
                    [md_record_written_timestamp],
                    [md_record_written_pipeline_id],
                    [md_transformation_job_id],
                    [md_source_system],
                    [sample_flag] ,
                    [promotion_code],
                    [return_value_tax],
                    [cancellation_value_tax],
                    [discount_type],
                    [discount_percentage],
                    [orig_line_value_pre_discounts]	
					) 
					VALUES (
					[SourceTbl].[orderid],
                    [SourceTbl].[retail_transaction_line_itemid],
                    [SourceTbl].[revenue_tax_exc_local],
                    [SourceTbl].[revenue_tax_inc_local],
                    [SourceTbl].[revenue_tax_exc_AUD],
                    [SourceTbl].[revenue_tax_inc_AUD],
                    [SourceTbl].[tax_amount],
                    [SourceTbl].[tax_amount_AUD],
                    [SourceTbl].[sales_units],
                    [SourceTbl].[discounted_price],
                    [SourceTbl].[product_code],
                    [SourceTbl].[product_variant_type],
                    [SourceTbl].[source_system],
                    [SourceTbl].[ingestion_timestamp],
                    [SourceTbl].[create_date_purchase],
                    [SourceTbl].[return_flag],
                    [SourceTbl].[return_qty],
                    [SourceTbl].[return_value],
                    [SourceTbl].[return_shipping_flag],
                    [SourceTbl].[return_shipping_value],
                    [SourceTbl].[return_date],
                    [SourceTbl].[cancelled_flag],
                    [SourceTbl].[cancellation_qty],
                    [SourceTbl].[cancellation_value],
                    [SourceTbl].[cancellation_shipping_flag],
                    [SourceTbl].[cancellation_shipping_value],
                    [SourceTbl].[cancellation_date],		
                    [SourceTbl].[md_record_written_timestamp],
                    [SourceTbl].[md_record_written_pipeline_id],
                    [SourceTbl].[md_transformation_job_id],
                    [SourceTbl].[md_source_system],
                    [SourceTbl].[sample_flag],
                    [SourceTbl].[promotion_code],
                    [SourceTbl].[return_value_tax],
                    [SourceTbl].[cancellation_value_tax],
                    [SourceTbl].[discount_type],
                    [SourceTbl].[discount_percentage],
                    [SourceTbl].[orig_line_value_pre_discounts] 
					)
            -- Does not update std.purchase_record_line_item if CEGID EOD data already present
			WHEN MATCHED AND [TargetTbl].[md_source_system] <> 'CEGID'  THEN
				UPDATE SET 
					[orderid] = [SourceTbl].[orderid],
                    [retail_transaction_line_itemid] = [SourceTbl].[retail_transaction_line_itemid],
                    [revenue_tax_exc_local] = [SourceTbl].[revenue_tax_exc_local],
                    [revenue_tax_inc_local] = [SourceTbl].[revenue_tax_inc_local],
                    [revenue_tax_exc_AUD] = [SourceTbl].[revenue_tax_exc_AUD],
                    [revenue_tax_inc_AUD] = [SourceTbl].[revenue_tax_inc_AUD],
                    [tax_amount] = [SourceTbl].[tax_amount],
                    [tax_amount_AUD] = [SourceTbl].[tax_amount_AUD],
                    [sales_units] = [SourceTbl].[sales_units],
                    [discounted_price] = [SourceTbl].[discounted_price],
                    [product_code] = [SourceTbl].[product_code],
                    [product_variant_type] = [SourceTbl].[product_variant_type],
                    [source_system] = [SourceTbl].[source_system],
                    [ingestion_timestamp] = [SourceTbl].[ingestion_timestamp],
                    [create_date_purchase] = [SourceTbl].[create_date_purchase],
                    [return_flag] = [SourceTbl].[return_flag],
                    [return_qty] = [SourceTbl].[return_qty],
                    [return_value] = [SourceTbl].[return_value],
                    [return_shipping_flag] = [SourceTbl].[return_shipping_flag],
                    [return_shipping_value] = [SourceTbl].[return_shipping_value],
                    [return_date] = [SourceTbl].[return_date],
                    [cancelled_flag] = [SourceTbl].[cancelled_flag],
                    [cancellation_qty] = [SourceTbl].[cancellation_qty],
                    [cancellation_value] = [SourceTbl].[cancellation_value],
                    [cancellation_shipping_flag] = [SourceTbl].[cancellation_shipping_flag],
                    [cancellation_shipping_value] = [SourceTbl].[cancellation_shipping_value],
                    [cancellation_date]  = [SourceTbl].[cancellation_date] ,
                    [md_record_written_timestamp] = [SourceTbl].[md_record_written_timestamp],
                    [md_record_written_pipeline_id] = [SourceTbl].[md_record_written_pipeline_id],
                    [md_transformation_job_id] = [SourceTbl].[md_transformation_job_id],
                    [md_source_system] = [SourceTbl].[md_source_system],
                    [sample_flag] = [SourceTbl].[sample_flag],
                    [promotion_code] = [SourceTbl].[promotion_code],
                    [return_value_tax] = [SourceTbl].[return_value_tax],
                    [cancellation_value_tax] = [SourceTbl].[cancellation_value_tax],
                    [discount_type] = [SourceTbl].[discount_type],
                    [discount_percentage] = [SourceTbl].[discount_percentage],
                    [orig_line_value_pre_discounts] = [SourceTbl].[orig_line_value_pre_discounts]

			OPTION (LABEL = 'AADPSTDLINEITM');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDLINEITM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			--------------------maintaining history-----------------------
		--	Insert into std.purchase_record_line_item_history
		--	select * from std.purchase_record_line_item;

			TRUNCATE TABLE [stage].[line_item_union_sources] ;
			UPDATE STATISTICS [std].[purchase_record_line_item];
	END
	ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			SELECT @newrec=max(md_record_written_timestamp) from std.purchase_record_line_item
			select @onlydate=CAST(@newrec as date);
			delete from std.purchase_record_line_item where md_record_written_timestamp=@newrec;
		--	delete from std.purchase_record_line_item_history where md_record_written_timestamp=@newrec;
		END
END TRY
BEGIN CATCH
		--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_purchase_record_line_item' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

END CATCH
END