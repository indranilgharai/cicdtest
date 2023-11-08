/**** Object:  StoredProcedure [cons_customer].[sp_sales_detail_time]    Script Date: 4/11/2022 2:32:21 PM ******/
/**** Modified  StoredProcedure  [Added logic to add tax amount to retailPro Japan Stores]  Modified Date: 20/06/2022 4:42:21 PM ******/
/**** Modified StoredProcedure [Added logic for removal of clickandcollect duplicates orders] Modified Date: 27/06/2022 4:42:21 PM ******/
/**** Modified StoredProcedure [Updated logic to include shipped_date] Modified Date: 09/07/2022 2:42:21 PM ******/
/**** Modified StoredProcedure [Updated load type from full load to delta load] Modified Date: 28/09/2022 2:42:21 PM ******/
/**** Modified  StoredProcedure  [Removed logic to add tax amount to retailPro Japan Stores]  Modified Date: 12/10/2022 4:42:21 PM ******/
--Updated data load type from full load to delta load
/**** Modified  StoredProcedure  [Added logic to add consultant_id column]  Modified Date: 19/01/2023 4:42:21 PM ******/
/**** Modified  StoredProcedure  [Modified the logic for staff_sale]  Modified Date: 02/02/2023 4:42:21 PM ******/
/**** Modified  StoredProcedure  [Modified the dedup logic to order by written timestamp first]  Modified Date: 13/06/2023 9:40:00 AM ******/
/**** Modified  StoredProcedure  [Modified Merge logic and simplified queries by moving channel_type case when and CLICKCOLLECT filter]  Modified Date: 31/07/2023 4:42:00 PM ******/
/**** Modified: Added 'bundle_sku_line_no' and 'bundle_sku_code', added merge logic to enable inclusion of bundle items    Script Date: 10/10/2023 6:00:00 PM  Modified By: Patrick Lacerna ****/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_customer].[sp_sales_detail_time] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			DECLARE @max_ingestion_date_cons [varchar](500)
			DECLARE @written_timestamp [varchar](500)
			SELECT 
				@max_ingestion_date_cons = max(CAST([md_record_written_timestamp] as date))
				,@written_timestamp = getDate()
			FROM cons_customer.sales_detail_time;

			IF OBJECT_ID('tempdb..#sales_detail_time') IS NOT  NULL
			BEGIN
				DROP TABLE #sales_detail_time
			END
			CREATE TABLE #sales_detail_time
			WITH
			(
				DISTRIBUTION = HASH(transaction_line_item_id),
				CLUSTERED COLUMNSTORE INDEX
			) AS 

			WITH line AS (
				SELECT 
					orderid AS order_id,
					retail_transaction_line_itemid AS transaction_line_item_id
					,bundle_sku_line_no as bundle_sku_line_no
					,case when source_system = 'HYBRIS' and return_flag='Y' then (revenue_tax_exc_local-return_value) 
						when  source_system = 'HYBRIS' and cancelled_flag='Y' then (revenue_tax_exc_local-cancellation_value) 
						when  source_system = 'HYBRIS' and (return_flag='Y' and cancelled_flag='Y') then (revenue_tax_exc_local-return_value-cancellation_value)
						else revenue_tax_exc_local end AS revenue_tax_exc_local
					,case when  source_system = 'HYBRIS' and return_flag='Y' then (revenue_tax_inc_local-return_value_tax) 
						when  source_system = 'HYBRIS' and cancelled_flag='Y' then (revenue_tax_inc_local-cancellation_value_tax) 
						when  source_system = 'HYBRIS' and (return_flag='Y' and cancelled_flag='Y') then (revenue_tax_inc_local-return_value_tax-cancellation_value_tax) 
						else revenue_tax_inc_local end AS revenue_tax_inc_local
					,case when  source_system = 'HYBRIS' and   return_flag='Y' then (abs(sales_units)-return_qty) 
						when   source_system = 'HYBRIS' and  cancelled_flag='Y' then (abs(sales_units)-cancellation_qty) 
						when  source_system = 'HYBRIS' and  (return_flag='Y' and cancelled_flag='Y') then (abs(sales_units)-return_qty-cancellation_qty) 
						when  source_system = 'HYBRIS' then  abs(sales_units)
						else sales_units end AS sales_units
					,case when abs(sales_units)=0 then 0
						when return_flag='Y' then tax_amount-((tax_amount/abs(sales_units))*return_qty) 
						when cancelled_flag='Y' then tax_amount-((tax_amount/abs(sales_units))*cancellation_qty) 
						when (return_flag='Y' and cancelled_flag='Y') then (tax_amount-((tax_amount/abs(sales_units))*return_qty) -((tax_amount/abs(sales_units))*cancellation_qty) )
						else tax_amount end AS tax_amount
					,case when return_flag='Y' then (discounted_price-return_value) 
						when cancelled_flag='Y' then (discounted_price-cancellation_value) 
						when (return_flag='Y' and cancelled_flag='Y') then (discounted_price-return_value-cancellation_value)
						else discounted_price end AS discounted_price
					,return_flag as return_flag
					,return_qty as return_qty
					,return_value as return_value
					,return_shipping_flag as return_shipping_flag
					,return_shipping_value as return_shipping_value
					,return_date as return_date
					,cancelled_flag as cancelled_flag
					,cancellation_qty as cancellation_qty
					,cancellation_value as cancellation_value
					,cancellation_shipping_flag as cancellation_shipping_flag
					,cancellation_shipping_value as cancellation_shipping_value
					,cancellation_date as cancellation_date
					,product_code as product_code
					,bundle_sku_code as bundle_sku_code
					,sample_flag as sample_flag
					,promotion_code as promotion_code
					,return_value_tax as return_value_tax
					,cancellation_value_tax as cancellation_value_tax
				from [std].[purchase_record_line_item]
				where cast (ingestion_timestamp as date) between @max_ingestion_date_cons and  DATEADD(day, +1, CAST(GETDATE() AS date))
			) 
			, sdt_tbl as (
				SELECT DISTINCT 
					line.[order_id] AS order_id
					,pr.[orderid] AS source_system_order_id
					,line.transaction_line_item_id AS transaction_line_item_id
					,line.bundle_sku_line_no AS bundle_sku_line_no
					,line.[revenue_tax_exc_local] AS revenue_tax_exc_local
					,line.[revenue_tax_inc_local] AS revenue_tax_inc_local
					,case when pr.currency_code='AUD' then revenue_tax_exc_local else (cast(revenue_tax_exc_local as float)/cast(exrate.ex_rate as FLOAT)) end as revenue_tax_exc_AUD
					,case when pr.currency_code='AUD' then revenue_tax_inc_local else (cast(revenue_tax_inc_local as float)/cast(exrate.ex_rate as FLOAT)) end as revenue_tax_inc_AUD
					,line.[tax_amount] AS tax_amount
					,case when pr.currency_code='AUD' then tax_amount else (cast(tax_amount as float)/cast(exrate.ex_rate as FLOAT)) end as tax_amount_AUD
					,coalesce (pr.customer_id,pr.source_system_customer_id,'') AS customer_id
					,case when pr.order_type = 'ClickandCollect' then 'Retail' else pr.channel_id end channel_type
					,ISNULL(sfmc.rfv_class,'') AS customer_RFV_id
					,ISNULL(sfmc.rfv_segment_name,'')  AS customer_RFV_description
					,(CASE WHEN sfmc.optinEmail = 'Active' OR sfmc.optinMobile = 'Active' THEN 'Y' ELSE 'N' END ) AS active_subscriber
					,(CASE WHEN ISNULL(pr.Customer_ID, '') = '' THEN 'unlinked'
						WHEN pr.Customer_ID like 'WI00%' THEN 'unlinked'
						ELSE 'linked' END ) AS [Customer_Type_Linked_Unlinked]
					,prd.category AS product_category
					,prd.sub_category AS product_sub_category
					,line.product_code AS SKU_number
					,line.bundle_sku_code AS bundle_sku_code
					,sub.sbs_region AS region
					,sub.sbs_name AS subsidiary
					,storex.store_address1
					,storex.store_address2
					,storex.store_postcode
					,(CASE WHEN pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
						WHEN pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
						WHEN pr.source_system = 'HYBRIS' THEN CAST(ISNULL(pr.fulfillment_location_code, ISNULL(pr.location_code, '999')) AS VARCHAR(50))
						ELSE NULL END) AS store_fulfillment
					,st.store_name as fulfillment_store_name
					,pr.store_name AS store_name
					,(CASE WHEN pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
						WHEN pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
						WHEN pr.source_system = 'HYBRIS' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
						ELSE NULL END) AS origin_store
					,storex.store_name as origin_store_name
					,pr.storx_sbs_no AS subsidiary_no
					-----------------excluding Voucher from order_total --------------
					,case when prd.category is null then pr.Price_local 
							when prd.category<>'Voucher' then pr.Price_local else 0.000 end AS order_total_local
					,case when prd.category is null then pr.price 
							when prd.category<>'Voucher' then pr.price else 0.000 end AS order_total_AUD
					,case when line.sample_flag='Y' then 0 else line.[sales_units] end AS sales_units
					,pr.[create_date_purchase] AS receipt_date
					,CASE WHEN pr.source_system = 'HYBRIS' AND (pr.OrderStatus = 'SHIPPED' or pr.OrderStatus = 'DELIVERED' or pr.OrderStatus = 'COMPLETED' or pr.OrderStatus = 'RETURNED') THEN shipped_date
						ELSE NULL END AS shipped_date
					,pr.currency_code AS currency_code
					,sub.sbs_currency_name AS currency_type
					,discounted_price  AS discounted_price
					--,ISNULL(fps.is_aesop_employee, 'N') AS staff_sale
					-- modified the logic to derive staff_sale from sfmc_voc and also fps_person_alias
					,CASE WHEN ISNULL(fps.is_aesop_employee, 'N')='Y' OR sfmcvoc.email like '%aesop.com' THEN 'Y'
					ELSE 'N' END AS staff_sale
					,ISNULL(a.Replenishment, 'N') AS product_replenishment
					,ISNULL((CASE WHEN cast(create_date_purchase AS DATE) <= cast(dateadd(month, 3, f_date.First_purchase_date) AS DATE) THEN 'Y' ELSE 'N' END), 'N')  AS new_to_aesop
					------------datetime conversion to local-----------
					,cast(switchoffset(cast(create_date_purchase AS DATETIME), substring(cast(create_date_purchase AS VARCHAR(50)), 29, 6)) AS DATETIME) AS transaction_date_time_local
					,dt.incr_date AS transaction_date_local
					,day_time.hr_24 AS transaction_time_local
					,dt.day_of_week transaction_day_local
					,pr.[create_date_purchase] AS standardised_time_stamp	
					,ISNULL(cat.customer_new_to_category,'N') AS Customer_new_to_category
					,line.sample_flag AS sample_flag
					,(CASE WHEN ISNULL(pr.is_gift_card_order, '') = 'Y' THEN 'Y' ELSE 'N' END) AS gift_flag
					,ISNULL(ch.[customer_channel_type], '')  AS [customer_channel_type]
					,ISNULL(fps.customer_group_id, '')  as customer_discount_group
					,CASE WHEN pr.source_system = 'CEGID' THEN 'Retail'
						WHEN pr.source_system = 'RETAILPRO' THEN 'Retail'
						WHEN pr.source_system = 'HYBRIS' THEN pr.order_type end as order_type
					------------- return related details-------------
					,ISNULL(line.return_flag, 'N') as return_flag
					,case when return_flag='Y' then line.return_qty else 0 end  as return_qty
					,case when return_flag='Y' then line.return_value else 0.000 end as return_value
					,ISNULL(line.return_shipping_flag, 'N') as return_shipping_flag
					,case when line.return_shipping_flag='Y' then pr.order_shipping_total else 0.000 end as return_shipping_value
					,line.return_date as return_date
					--------------cancellation related details-------------
					,ISNULL(line.cancelled_flag, 'N') as cancelled_flag
					,case when cancelled_flag='Y' then line.cancellation_qty else 0 end as cancellation_qty
					,case when cancelled_flag='Y' then line.cancellation_value else 0.0 end as cancellation_value
					,ISNULL(line.cancellation_shipping_flag, 'N') as cancellation_shipping_flag
					,case when line.cancellation_shipping_flag='Y' then pr.order_shipping_total else 0.000 end  as cancellation_shipping_value
					,line.cancellation_date as cancellation_date	
					-------------metadata fields----------
					,@written_timestamp AS md_record_written_timestamp
					,@pipelineid AS md_record_written_pipeline_id
					,@jobid AS md_transformation_job_id
					,line.promotion_code as promotion_code
					,line.return_value_tax as return_value_tax
					,line.cancellation_value_tax as cancellation_value_tax
					,pr.sales_consultant as consultant_id
				FROM std.purchase_record pr WITH (NOLOCK) 
				INNER JOIN line WITH (NOLOCK)  ON pr.[orderid] = line.[order_id]
				INNER JOIN [std].[date_dim] dt WITH (NOLOCK)  ON dt.incr_date = cast(switchoffset(cast(create_date_purchase AS DATETIME), substring(cast(create_date_purchase AS VARCHAR(50)), 29, 6)) AS DATE)
				INNER JOIN [std].[time_dim] day_time WITH (NOLOCK)  ON day_time.hr_24 = datepart(hour, cast(switchoffset(cast(create_date_purchase AS DATETIME), substring(cast(create_date_purchase AS VARCHAR(50)), 29, 6)) AS DATETIME)) + 1
				LEFT JOIN (select * from (select sbs_no,ex_rate,year,month_no,row_number() over(partition by sbs_no order by year desc,month_no desc) rwno from  [std].[exchange_rate_x] )a where rwno=1 ) exrate ON cast(pr.storx_sbs_no AS INT) = cast(exrate.sbs_no AS INT)
				LEFT JOIN std.[fps_person_alias] fps WITH (NOLOCK) ON cast(trim(ISNULL(fps.customer_id, '')) AS VARCHAR) = cast(trim(ISNULL(pr.customer_id, '')) AS VARCHAR)
				LEFT JOIN std.sfmc_customer sfmc WITH (NOLOCK) ON cast(trim(sfmc.contactkey) AS VARCHAR) = cast(trim(pr.customer_id) AS VARCHAR)			
				LEFT JOIN std.product_X prd WITH (NOLOCK) ON prd.description1 = line.product_code
				LEFT JOIN std.subsidiary_X sub WITH (NOLOCK) ON pr.storx_sbs_no = sub.sbs_no
				LEFT JOIN (select distinct address1 as store_address1,address2 as store_address2,postcode as store_postcode,location_code,store_name from std.store_x WITH (NOLOCK)) storex 
					on pr.location_code =storex.location_code 
				LEFT JOIN (select distinct location_code,store_name from std.store_x WITH (NOLOCK)) st   
					on CAST(ISNULL(pr.fulfillment_location_code, ISNULL(pr.location_code, '999')) AS VARCHAR(50)) =st.location_code 
				-------------------Derive Replenishment Flag logic---------------------------------		
				LEFT JOIN (select customer_id,[product_code],orderid,case when rec>1 then 'Y' else 'N' end as Replenishment from(
							select distinct  pr.customer_id,pr.orderid,pl.[product_code],
							dense_rank() OVER (PARTITION BY pr.customer_id,pl.[product_code] ORDER BY pr.create_date_purchase asc) AS rec 
							FROM std.purchase_record pr WITH (NOLOCK) 
							INNER JOIN [std].[purchase_record_line_item] pl WITH (NOLOCK)  ON pr.orderID = pl.orderid 
							where customer_id <>'' and pl.sample_flag<>'Y')x
						) a   ON pr.orderid=a.orderid and LINE.[product_code] = a.[product_code] AND a.customer_id = pr.customer_id	
				---------- derive New to Aesop(first purchase date for Customer)---------------------------
				LEFT JOIN (SELECT customer_id,create_date_purchase AS First_purchase_date FROM (
							SELECT row_number() OVER(PARTITION BY customer_id ORDER BY create_date_purchase ASC
							) rowval,* FROM (SELECT DISTINCT customer_id,create_date_purchase FROM std.purchase_record WITH (NOLOCK)
								where customer_id <>'') pur
							) a WHERE a.rowval = 1
						) f_date  ON pr.customer_id = f_date.customer_id
				--------------------derive channel_type------------------------
				LEFT JOIN (select customer_id,case when count(channel_id) > 1 THEN 'Multi-Channel Customer'
							WHEN count(channel_id) = 1 THEN 'Single-Channel Customer' END  AS [customer_channel_type]
							from (select distinct customer_id,channel_id from std.purchase_record WITH (NOLOCK) where customer_id <>'')a 
							group by customer_id 
						) ch   ON ch.customer_id = pr.customer_id
				---------------------to derive email from sfmc_svoc-----------------------
				LEFT JOIN std.sfmc_svoc sfmcvoc WITH (NOLOCK) ON cast(trim(sfmcvoc.contactkey) AS VARCHAR) = cast(trim(pr.customer_id) AS VARCHAR)		
				-----------------Derive customer_new_to_product_category------------------------
				LEFT JOIN (select customer_id,product_category,orderid,case when rec=1 then 'Y' else 'N' end as customer_new_to_category 
							from(SELECT pr.customer_id,prd.category AS product_category,pr.orderid,
							dense_rank() OVER (PARTITION BY pr.customer_id,prd.category ORDER BY pr.create_date_purchase asc) AS rec
							FROM (select distinct orderid,customer_id,create_date_purchase from std.purchase_record WITH (NOLOCK)) pr
							INNER JOIN [std].[purchase_record_line_item] pl WITH (NOLOCK) ON pr.orderID = pl.orderid
							LEFT JOIN std.product_X prd WITH (NOLOCK) ON prd.description1 = pl.product_code	
							where customer_id<>'' and pl.sample_flag<>'Y')a
						) cat   ON pr.orderid=cat.orderid and prd.category = cat.product_category AND cat.customer_id = pr.customer_id
				WHERE cast (ingestion_timestamp as date) between @max_ingestion_date_cons and DATEADD(day, +1, CAST(GETDATE() AS date))
				and (line.[order_id] not in (select distinct line.[order_id] from line  where UPPER(line.product_code) = 'CLICKCOLLECT'))
			)

			SELECT * FROM sdt_tbl;

			DELETE FROM [cons_customer].[sales_detail_time]
			WHERE order_id in (
				SELECT DISTINCT order_id FROM #sales_detail_time
			)
			AND transaction_line_item_id NOT IN (
				SELECT DISTINCT transaction_line_item_id FROM #sales_detail_time
			);

			MERGE [cons_customer].[sales_detail_time] AS TargetTbl
			USING #sales_detail_time AS SourceTbl
			ON SourceTbl.order_id = TargetTbl.order_id
			AND SourceTbl.transaction_line_item_id = TargetTbl.transaction_line_item_id
			-- Items in bundle share line_item_id, need to add bundle_sku_line_no for uniqueness
			AND ISNULL(SourceTbl.bundle_sku_line_no,'') = ISNULL(TargetTbl.bundle_sku_line_no,'')

			-- Insert streaming data if record does not exist yet
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (
					order_id,
					source_system_order_id,
					transaction_line_item_id,
					bundle_sku_line_no,
					revenue_tax_exc_local,
					revenue_tax_inc_local,
					revenue_tax_exc_AUD,
					revenue_tax_inc_AUD,
					tax_amount,
					tax_amount_AUD,
					customer_id,
					channel_type,
					customer_RFV_id,
					customer_RFV_description,
					active_subscriber,
					Customer_Type_Linked_Unlinked,
					product_category,
					product_sub_category,
					SKU_number,
					bundle_sku_code,
					region,
					subsidiary,
					store_address1,
					store_address2,
					store_postcode,
					store_fulfillment,
					fulfillment_store_name,
					store_name,
					origin_store,
					origin_store_name,
					subsidiary_no,
					order_total_local,
					order_total_aud,
					sales_units,
					receipt_date,
					shipped_date,
					currency_code,
					currency_type,
					discounted_price,
					staff_sale,
					product_replenishment,
					new_to_aesop,
					transaction_date_time_local,
					transaction_date_local,
					transaction_time_local,
					transaction_day_local,
					standardised_time_stamp,
					customer_new_to_category,
					sample_flag,
					Gift_Flag,
					customer_channel_type,
					customer_discount_group,
					order_type,
					return_flag,
					return_qty,
					return_value,
					return_shipping_flag,
					return_shipping_value,
					return_date,
					cancelled_flag,
					cancellation_qty,
					cancellation_value,
					cancellation_shipping_flag,
					cancellation_shipping_value,
					cancellation_date,
					md_record_written_timestamp,
					md_record_written_pipeline_id,
					md_transformation_job_id,
					promotion_code,
					return_value_tax,
					cancellation_value_tax,
					consultant_id
					) 
					VALUES (
					SourceTbl.order_id,
					SourceTbl.source_system_order_id,
					SourceTbl.transaction_line_item_id,
					SourceTbl.bundle_sku_line_no,
					SourceTbl.revenue_tax_exc_local,
					SourceTbl.revenue_tax_inc_local,
					SourceTbl.revenue_tax_exc_AUD,
					SourceTbl.revenue_tax_inc_AUD,
					SourceTbl.tax_amount,
					SourceTbl.tax_amount_AUD,
					SourceTbl.customer_id,
					SourceTbl.channel_type,
					SourceTbl.customer_RFV_id,
					SourceTbl.customer_RFV_description,
					SourceTbl.active_subscriber,
					SourceTbl.Customer_Type_Linked_Unlinked,
					SourceTbl.product_category,
					SourceTbl.product_sub_category,
					SourceTbl.SKU_number,
					SourceTbl.bundle_sku_code,
					SourceTbl.region,
					SourceTbl.subsidiary,
					SourceTbl.store_address1,
					SourceTbl.store_address2,
					SourceTbl.store_postcode,
					SourceTbl.store_fulfillment,
					SourceTbl.fulfillment_store_name,
					SourceTbl.store_name,
					SourceTbl.origin_store,
					SourceTbl.origin_store_name,
					SourceTbl.subsidiary_no,
					SourceTbl.order_total_local,
					SourceTbl.order_total_aud,
					SourceTbl.sales_units,
					SourceTbl.receipt_date,
					SourceTbl.shipped_date,
					SourceTbl.currency_code,
					SourceTbl.currency_type,
					SourceTbl.discounted_price,
					SourceTbl.staff_sale,
					SourceTbl.product_replenishment,
					SourceTbl.new_to_aesop,
					SourceTbl.transaction_date_time_local,
					SourceTbl.transaction_date_local,
					SourceTbl.transaction_time_local,
					SourceTbl.transaction_day_local,
					SourceTbl.standardised_time_stamp,
					SourceTbl.customer_new_to_category,
					SourceTbl.sample_flag,
					SourceTbl.Gift_Flag,
					SourceTbl.customer_channel_type,
					SourceTbl.customer_discount_group,
					SourceTbl.order_type,
					SourceTbl.return_flag,
					SourceTbl.return_qty,
					SourceTbl.return_value,
					SourceTbl.return_shipping_flag,
					SourceTbl.return_shipping_value,
					SourceTbl.return_date,
					SourceTbl.cancelled_flag,
					SourceTbl.cancellation_qty,
					SourceTbl.cancellation_value,
					SourceTbl.cancellation_shipping_flag,
					SourceTbl.cancellation_shipping_value,
					SourceTbl.cancellation_date,
					SourceTbl.md_record_written_timestamp,
					SourceTbl.md_record_written_pipeline_id,
					SourceTbl.md_transformation_job_id,
					SourceTbl.promotion_code,
					SourceTbl.return_value_tax,
					SourceTbl.cancellation_value_tax,
					SourceTbl.consultant_id
					)
			WHEN MATCHED THEN
				UPDATE SET 
					order_id = SourceTbl.order_id,
					source_system_order_id = SourceTbl.source_system_order_id,
					transaction_line_item_id = SourceTbl.transaction_line_item_id,
					bundle_sku_line_no = SourceTbl.bundle_sku_line_no,
					revenue_tax_exc_local = SourceTbl.revenue_tax_exc_local,
					revenue_tax_inc_local = SourceTbl.revenue_tax_inc_local,
					revenue_tax_exc_AUD = SourceTbl.revenue_tax_exc_AUD,
					revenue_tax_inc_AUD = SourceTbl.revenue_tax_inc_AUD,
					tax_amount = SourceTbl.tax_amount,
					tax_amount_AUD = SourceTbl.tax_amount_AUD,
					customer_id = SourceTbl.customer_id,
					channel_type = SourceTbl.channel_type,
					customer_RFV_id = SourceTbl.customer_RFV_id,
					customer_RFV_description = SourceTbl.customer_RFV_description,
					active_subscriber = SourceTbl.active_subscriber,
					Customer_Type_Linked_Unlinked = SourceTbl.Customer_Type_Linked_Unlinked,
					product_category = SourceTbl.product_category,
					product_sub_category = SourceTbl.product_sub_category,
					SKU_number = SourceTbl.SKU_number,
					bundle_sku_code = SourceTbl.bundle_sku_code,
					region = SourceTbl.region,
					subsidiary = SourceTbl.subsidiary,
					store_address1 = SourceTbl.store_address1,
					store_address2 = SourceTbl.store_address2,
					store_postcode = SourceTbl.store_postcode,
					store_fulfillment = SourceTbl.store_fulfillment,
					fulfillment_store_name = SourceTbl.fulfillment_store_name,
					store_name = SourceTbl.store_name,
					origin_store = SourceTbl.origin_store,
					origin_store_name = SourceTbl.origin_store_name,
					subsidiary_no = SourceTbl.subsidiary_no,
					order_total_local = SourceTbl.order_total_local,
					order_total_aud = SourceTbl.order_total_aud,
					sales_units = SourceTbl.sales_units,
					receipt_date = SourceTbl.receipt_date,
					shipped_date = SourceTbl.shipped_date,
					currency_code = SourceTbl.currency_code,
					currency_type = SourceTbl.currency_type,
					discounted_price = SourceTbl.discounted_price,
					staff_sale = SourceTbl.staff_sale,
					product_replenishment = SourceTbl.product_replenishment,
					new_to_aesop = SourceTbl.new_to_aesop,
					transaction_date_time_local = SourceTbl.transaction_date_time_local,
					transaction_date_local = SourceTbl.transaction_date_local,
					transaction_time_local = SourceTbl.transaction_time_local,
					transaction_day_local = SourceTbl.transaction_day_local,
					standardised_time_stamp = SourceTbl.standardised_time_stamp,
					customer_new_to_category = SourceTbl.customer_new_to_category,
					sample_flag = SourceTbl.sample_flag,
					Gift_Flag = SourceTbl.Gift_Flag,
					customer_channel_type = SourceTbl.customer_channel_type,
					customer_discount_group = SourceTbl.customer_discount_group,
					order_type = SourceTbl.order_type,
					return_flag = SourceTbl.return_flag,
					return_qty = SourceTbl.return_qty,
					return_value = SourceTbl.return_value,
					return_shipping_flag = SourceTbl.return_shipping_flag,
					return_shipping_value = SourceTbl.return_shipping_value,
					return_date = SourceTbl.return_date,
					cancelled_flag = SourceTbl.cancelled_flag,
					cancellation_qty = SourceTbl.cancellation_qty,
					cancellation_value = SourceTbl.cancellation_value,
					cancellation_shipping_flag = SourceTbl.cancellation_shipping_flag,
					cancellation_shipping_value = SourceTbl.cancellation_shipping_value,
					cancellation_date = SourceTbl.cancellation_date,
					md_record_written_timestamp = SourceTbl.md_record_written_timestamp,
					md_record_written_pipeline_id = SourceTbl.md_record_written_pipeline_id,
					md_transformation_job_id = SourceTbl.md_transformation_job_id,
					promotion_code = SourceTbl.promotion_code,
					return_value_tax = SourceTbl.return_value_tax,
					cancellation_value_tax = SourceTbl.cancellation_value_tax,
					consultant_id = SourceTbl.consultant_id;

			UPDATE STATISTICS [cons_customer].[sales_detail_time];
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPCONSSALES'

			EXEC meta_ctl.sp_row_count @jobid ,@step_number ,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_customer.sales_detail_time;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM cons_customer.sales_detail_time WHERE md_record_written_timestamp=@newrec;
		END
	END TRY
	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.sp_sales_detail_time' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END