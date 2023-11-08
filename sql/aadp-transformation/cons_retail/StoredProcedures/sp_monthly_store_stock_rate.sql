/****** Object:  StoredProcedure [cons_retail].[sp_monthly_store_stock_rate]    Script Date: 12/12/2022 3:21:37 PM ******/
/******Modified  Object:  StoredProcedure [cons_retail].[sp_monthly_store_stock_rate] to handle duplicates  Script Date: 11/05/2023 3:11:37 PM ******/
/****** Modified SP: Date: 27/06/2023 Modified SP to consider sales made from digital warehouses which are fulfilled out of virtual warehouses ******/
/****** Modified SP: Date: 26/07/2023 Modified SP to add additional fields for days instore intrade and 3 months metrics calculations ******/
/****** Modified: Re-built SP to optimise and simplify query   Script Date: 9/06/2023 08:00:00 PM   Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_monthly_store_stock_rate] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			BEGIN
				TRUNCATE TABLE [cons_retail].[monthly_store_stock_rate];
				
				----- PARAMETERS -----
				/*
				base_last_x_months: last 25 months used so that date-12months can calculate last12months values accurately 
				return_last_x_months: will return x months in final select
				*/
				DECLARE @base_last_x_months AS int = 25 
				DECLARE @return_last_x_months AS int = 13;

				----- CREATE TEMP TABLES -----
				/* 
				--- #BASE_SDT_1 ---
				Enhances sales transactions with sp_sales_detail_time logic and maps product_code to product_x merge_code 
				*/
				IF OBJECT_ID('tempdb..#BASE_SDT_1') IS NOT NULL BEGIN DROP TABLE #BASE_SDT_1 END
				CREATE TABLE #BASE_SDT_1
				WITH (DISTRIBUTION = HASH ([orderid]), HEAP) AS
				SELECT
					DISTINCT line.orderid
					, line.retail_transaction_line_itemid
					, CASE
						WHEN pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
						WHEN pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
						WHEN pr.source_system = 'HYBRIS' THEN CAST(
							ISNULL(
								pr.fulfillment_location_code,
								ISNULL(pr.location_code, '999')
							) AS VARCHAR(50)
						)
						ELSE NULL
					END AS store_fulfillment
					, prodx.merge_code AS SKU_number
					, CAST(
						CASE
							WHEN (pr.channel_id = 'Digital') THEN CASE
								WHEN left(pr.orderid, 1) = 'H' THEN CASE
									WHEN pr.source_system = 'HYBRIS'
									AND (
										pr.OrderStatus = 'SHIPPED'
										OR pr.OrderStatus = 'DELIVERED'
										OR pr.OrderStatus = 'COMPLETED'
										OR pr.OrderStatus = 'RETURNED'
									) THEN shipped_date
									ELSE NULL
								END
								ELSE pr.create_date_purchase
							END
							ELSE pr.create_date_purchase
						END AS date
					) AS receipt_date
					, CASE
						WHEN line.source_system = 'HYBRIS' AND (return_flag = 'Y' AND cancelled_flag = 'Y')
							THEN (ABS(sales_units) - return_qty - cancellation_qty)
						WHEN line.source_system = 'HYBRIS' AND return_flag = 'Y' 
							THEN (ABS(sales_units) - return_qty)
						WHEN line.source_system = 'HYBRIS' AND cancelled_flag = 'Y' 
							THEN (ABS(sales_units) - cancellation_qty)
						WHEN line.source_system = 'HYBRIS' 
							THEN ABS(sales_units)
						ELSE sales_units
					END AS sales_units
					, CASE
						WHEN line.source_system = 'HYBRIS' AND (return_flag = 'Y' AND cancelled_flag = 'Y') 
							THEN (revenue_tax_exc_AUD - return_value - cancellation_value)
						WHEN line.source_system = 'HYBRIS' AND return_flag = 'Y' 
							THEN (revenue_tax_exc_AUD - return_value)
						WHEN line.source_system = 'HYBRIS' AND cancelled_flag = 'Y' 
							THEN (revenue_tax_exc_AUD - cancellation_value)
						ELSE revenue_tax_exc_AUD
					END AS revenue_tax_exc_AUD
				FROM
					[std].[purchase_record] pr
					INNER JOIN [std].[purchase_record_line_item] line ON pr.orderid = line.orderid
					LEFT JOIN [std].[product_x] prodx ON line.product_code = prodx.description1
				WHERE
					pr.create_date_purchase >= DATEADD(MONTH, -@base_last_x_months, getDate());

				/* 
				--- #BASE_SDT_2 --- 
				Enhances BASE_SDT_1 to remap store_fullfilment with virtual_warehouses
				Base to calculate metrics: 
					nDays_storeTraded
					total_sales_units
					total_sales_revAUD
				*/
				IF OBJECT_ID('tempdb..#BASE_SDT_2') IS NOT NULL BEGIN DROP TABLE #BASE_SDT_2 END
				CREATE TABLE #BASE_SDT_2
				WITH (DISTRIBUTION = HASH ([incr_date]),HEAP) AS
				SELECT
					CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, sdt.receipt_date), 0) AS date) AS incr_date
					, ISNULL(
						dl.virtual_warehouse_code_location_code,
						sdt.store_fulfillment
					) AS store_fulfillment
					, sdt.SKU_number
					, sdt.receipt_date
					, sdt.sales_units
					, sdt.revenue_tax_exc_AUD
				FROM
					#BASE_SDT_1 sdt
					LEFT JOIN [stage].[dwh_digital_locations] dl ON sdt.store_fulfillment = dl.warehouse_code_location_code;

				/* 
				--- #BASE_TBL ---
				Forms base fact table by cross joining dates, products (merge_code), and locations (with virtual warehouse mapping)
				Base to calculate metrics: 
					nDays
					nDays_OutOfStock
				*/
				IF OBJECT_ID('tempdb..#BASE_TBL') IS NOT NULL BEGIN DROP TABLE #BASE_TBL END
				CREATE TABLE #BASE_TBL WITH (DISTRIBUTION = HASH ([date_loc_mergekey]),HEAP) AS
				SELECT
					CAST(CONCAT(incr_date, location_code, merge_code) AS VARCHAR(100)) AS date_loc_mergekey
					, *
				FROM
					(
						SELECT
							incr_date
							, DAY(EOMONTH(incr_date)) AS nDays_Month
							, DAY(EOMONTH(DATEADD(MONTH, -1, incr_date))) AS nDays_priorMonth
							, DATEDIFF(DAY,DATEADD(MONTH, -3, incr_date),EOMONTH(incr_date)) + 1 AS nDays_last3months
							, DATEDIFF(DAY,DATEADD(MONTH, -11, incr_date),EOMONTH(incr_date)) + 1 AS nDays_last12months
						FROM
							[std].[date_dim]
						WHERE
							RIGHT(incr_date, 2) = '01'
							AND DATEDIFF(MONTH, incr_date, getDate()) <= @base_last_x_months
							AND DATEDIFF(MONTH, incr_date, getDate()) >= 1
					) dateKey
					CROSS JOIN (
						SELECT
							DISTINCT location_code
						FROM
							[std].[store_x]
						WHERE
							location_code IN (
								SELECT
									DISTINCT store_fulfillment
								FROM
									#BASE_SDT_2
								WHERE
									receipt_date >= DATEADD(MONTH, -@return_last_x_months, getdate())
									AND revenue_tax_exc_AUD > 0
							)
					) LocationKey
					CROSS JOIN (
						SELECT
							DISTINCT merge_code
						FROM
							[std].[product_x]
						WHERE
							ISNULL(merge_code, '') != ''
					) Mergecode
				WHERE
					CONCAT(location_code, merge_code) NOT IN (
						SELECT
							DISTINCT CONCAT(locationid, Sku_Code)
						FROM
							[std].[dimitem_location]
						WHERE
							excluded_from_stock = 'Y'
					);

				/* 
				--- #BASE_NETSUITE_INV ---
				Enhances netsuite item inventory table with item_codes mapped to their product_x merge_code, 
				and joins with BASE_SDT_2 to check if store was trading based on receipt_dates
				Base to calculate metrics:
					days_inStock
					days_inStock_inTrade
					stock_on_hand_units
					stock_on_hand_value
				*/
				IF OBJECT_ID('tempdb..#BASE_NETSUITE_INV') IS NOT NULL BEGIN DROP TABLE #BASE_NETSUITE_INV END
				CREATE TABLE #BASE_NETSUITE_INV WITH (DISTRIBUTION = HASH ([incr_date]),HEAP) AS
				SELECT
					item_inv.store_warehouse_code
					, prodx.merge_code AS merge_code
					, item_inv.physical_inventory
					, CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, item_inv.soh_time_stamp), 0) AS date) AS incr_date
					, CASE
						WHEN ISNULL(item_inv.physical_inventory, 0) > 0 THEN 1
						ELSE 0
					END AS in_stock
					, CASE
						WHEN sdt_store_date.receipt_date IS NOT NULL THEN 1
						ELSE 0
					END AS in_trade
					, CASE
						WHEN ISNULL(item_inv.physical_inventory, 0) > 0
						AND sdt_store_date.receipt_date IS NOT NULL THEN 1
						ELSE 0
					END AS in_stock_and_trading
					, CASE
						WHEN item_inv.soh_time_stamp = CAST(
							DATEADD(MONTH, DATEDIFF(MONTH, 0, item_inv.soh_time_stamp), 0) AS date
						) THEN ISNULL(item_inv.physical_inventory, 0)
						ELSE 0
					END AS StartOfMonth_SOH
					, CASE
						WHEN item_inv.soh_time_stamp = EOMONTH(item_inv.soh_time_stamp) THEN ISNULL(item_inv.physical_inventory, 0)
						ELSE 0
					END AS EndOfMonth_SOH
				FROM
					[std].[netsuite_item_inventory] item_inv
					LEFT JOIN (
						SELECT
							DISTINCT receipt_date,
							store_fulfillment
						FROM
							#BASE_SDT_2) sdt_store_date ON item_inv.store_warehouse_code = sdt_store_date.store_fulfillment
						AND item_inv.soh_time_stamp = sdt_store_date.receipt_date
					LEFT JOIN [std].[product_x] prodx ON item_inv.item_code = prodx.description1
				WHERE
					item_inv.source = 'CEGID';

				----- AGGREGATE DATA -----
				/* sdt_days_traded: Separate CTE to calculate days_traded as granularity is date & store */
				WITH sdt_days_traded AS (
					SELECT
						sdt.store_fulfillment AS location_code
						, sdt.incr_date
						, COUNT(DISTINCT sdt.receipt_date) AS nDays_storeTraded
					FROM
						#BASE_SDT_2 sdt
					WHERE
						sdt.sales_units IS NOT NULL
					GROUP BY
						sdt.store_fulfillment,
						sdt.incr_date
				),

				/* sdt_agg: Aggregates BASE_SDT_2 with date & store & merge_code as granularity*/
				sdt_agg AS (
					SELECT
						DISTINCT CAST(CONCAT(sdt.incr_date,sdt.store_fulfillment,sdt.SKU_number) AS VARCHAR(100)) AS date_loc_mergekey
						, SUM(sdt.sales_units) AS sum_sales_units
						, SUM(sdt.revenue_tax_exc_AUD) AS sum_revenue_tax_exc_AUD
					FROM
						#BASE_SDT_2 sdt
					GROUP BY
						CAST(CONCAT(sdt.incr_date,sdt.store_fulfillment,sdt.SKU_number) AS VARCHAR(100))
				),

				/* inv_agg: Aggregates BASE_NETSUITE_INV with date & store & merge_code as granularity*/
				inv_agg AS (
					SELECT
						DISTINCT CAST(CONCAT(inv.incr_date, inv.store_warehouse_code, inv.merge_code) AS VARCHAR(100)) AS date_loc_mergekey
						, SUM(inv.in_stock_and_trading) AS days_in_stock_and_trading
						, SUM(inv.in_stock) AS days_in_stock
						, SUM(inv.StartOfMonth_SOH) AS sum_StartOfMonth_SOH
						, SUM(inv.StartOfMonth_SOH * it.cost) AS sum_StartOfMonth_SOH_value
						, SUM(inv.EndOfMonth_SOH) AS sum_EndOfMonth_SOH
						, SUM(inv.EndOfMonth_SOH * it.cost) AS sum_EndOfMonth_SOH_value
					FROM
						#BASE_NETSUITE_INV inv
						LEFT JOIN [std].[dimitem_location] it ON it.locationid = inv.store_warehouse_code
						AND it.sku_code = inv.merge_code
						AND inv.incr_date >= it.eff_from_date
						AND (
							inv.incr_date < it.eff_to_date
							OR it.eff_to_date IS NULL
						)
					GROUP BY
						CAST(CONCAT(inv.incr_date, inv.store_warehouse_code, inv.merge_code) AS VARCHAR(100))
				),

				----- JOIN BASE AND AGGREGATIONS -----
				main AS (
					SELECT
						-- Keys
						base.incr_date
						, base.location_code
						, base.merge_code
						, base.date_loc_mergekey
						-- Sales
						, sdt.sum_sales_units AS thisMonth_total_sales_units
						, LAG(sdt.sum_sales_units,1) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC) AS priorMonth_total_sales_units
						, SUM(sdt.sum_sales_units) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS last3Months_total_sales_units
						, SUM(sdt.sum_sales_units) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 12 PRECEDING and 1 PRECEDING) AS last12Months_total_sales_units
						-- Revenue
						, sdt.sum_revenue_tax_exc_AUD AS thisMonth_total_sales_revAUD
						, LAG(sdt.sum_revenue_tax_exc_AUD,1) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC) AS priorMonth_total_sales_revAUD
						, SUM(sdt.sum_revenue_tax_exc_AUD) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS last3Months_total_sales_revAUD
						, SUM(sdt.sum_revenue_tax_exc_AUD) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 12 PRECEDING and 1 PRECEDING) AS last12Months_total_sales_revAUD
						-- Stock
						, inv.days_in_stock AS days_inStock_Month
						, LAG(inv.days_in_stock,1) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC) AS priorMonth_days_inStock
						, SUM(inv.days_in_stock) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS last3Months_days_inStock
						, SUM(inv.days_in_stock) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 12 PRECEDING and 1 PRECEDING) AS last12Months_days_inStock
						-- In Stock In Trade
						, inv.days_in_stock_and_trading AS days_inStock_inTrade_Month
						, LAG(inv.days_in_stock_and_trading,1) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC) AS days_inStock_inTrade_priorMonth
						, SUM(inv.days_in_stock_and_trading) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS days_inStock_inTrade_last3months
						, SUM(inv.days_in_stock_and_trading) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 12 PRECEDING and 1 PRECEDING) AS days_inStock_inTrade_last12months
						-- Out of Stock
						, base.nDays_Month - inv.days_in_stock AS days_outOfStock_Month
						, base.nDays_priorMonth - LAG(inv.days_in_stock,1) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC) AS days_outOfStock_priorMonth
						, base.nDays_last3months - SUM(inv.days_in_stock) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS days_outOfStock_last3months
						, base.nDays_last12months - SUM(inv.days_in_stock) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 12 PRECEDING and 1 PRECEDING) AS days_outOfStock_last12months
						-- Days
						, nDays_Month
						, nDays_priorMonth
						, nDays_last3months
						, nDays_last12months
						-- Traded
						, traded.nDays_storeTraded AS nDays_storeTraded_Month
						, LAG(traded.nDays_storeTraded,1) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC) AS nDays_storeTraded_priorMonth
						, SUM(traded.nDays_storeTraded) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS nDays_storeTraded_last3months
						, SUM(traded.nDays_storeTraded) OVER (PARTITION BY base.merge_code, base.location_code ORDER BY base.incr_date ASC ROWS BETWEEN 12 PRECEDING and 1 PRECEDING) AS nDays_storeTraded_last12months
						-- Stock On Hand and Value
						, inv.sum_StartOfMonth_SOH AS stock_on_hand_units_startMonth
						, inv.sum_StartOfMonth_SOH_value AS stock_on_hand_value_startMonth
						, inv.sum_EndOfMonth_SOH AS stock_on_hand_units_endMonth
						, inv.sum_EndOfMonth_SOH_value AS stock_on_hand_value_endMonth
						-- Metadata
						, getDate() AS md_record_written_timestamp
						, @pipelineid   AS md_record_written_pipeline_id
						, @jobid  AS md_transformation_job_id
					FROM #BASE_TBL base
						LEFT JOIN sdt_agg sdt ON sdt.date_loc_mergekey = base.date_loc_mergekey
						LEFT JOIN inv_agg inv ON inv.date_loc_mergekey = base.date_loc_mergekey
						LEFT JOIN sdt_days_traded traded ON traded.location_code = base.location_code and traded.incr_date = base.incr_date
				)

				----- MERGE DATA TO TARGET -----
				INSERT INTO [cons_retail].[monthly_store_stock_rate]
				SELECT 
					DISTINCT main.incr_date
					, main.location_code
					, main.merge_code
					, main.date_loc_mergekey
					, ISNULL(main.thisMonth_total_sales_units, 0) AS thisMonth_total_sales_units
					, ISNULL(main.priorMonth_total_sales_units, 0) AS priorMonth_total_sales_units
					, ISNULL(main.last3Months_total_sales_units, 0) AS last3Months_total_sales_units
					, ISNULL(main.last12Months_total_sales_units, 0) AS last12Months_total_sales_units
					, ISNULL(main.thisMonth_total_sales_revAUD, 0) AS thisMonth_total_sales_revAUD
					, ISNULL(main.priorMonth_total_sales_revAUD, 0) AS priorMonth_total_sales_revAUD
					, ISNULL(main.last3Months_total_sales_revAUD, 0) AS last3Months_total_sales_revAUD
					, ISNULL(main.last12Months_total_sales_revAUD, 0) AS last12Months_total_sales_revAUD
					, ISNULL(main.days_inStock_Month, 0) AS days_inStock_Month
					, ISNULL(main.priorMonth_days_inStock, 0) AS priorMonth_days_inStock
					, ISNULL(main.last3Months_days_inStock, 0) AS last3Months_days_inStock
					, ISNULL(main.last12Months_days_inStock, 0) AS last12Months_days_inStock
					, ISNULL(main.days_inStock_inTrade_Month, 0) AS days_inStock_inTrade_Month
					, ISNULL(main.days_inStock_inTrade_priorMonth, 0) AS days_inStock_inTrade_priorMonth
					, ISNULL(main.days_inStock_inTrade_last3months, 0) AS days_inStock_inTrade_last3months
					, ISNULL(main.days_inStock_inTrade_last12months, 0) AS days_inStock_inTrade_last12months
					, ISNULL(main.days_outOfStock_Month, 0) AS days_outOfStock_Month
					, ISNULL(main.days_outOfStock_priorMonth, 0) AS days_outOfStock_priorMonth
					, ISNULL(main.days_outOfStock_last3months, 0) AS days_outOfStock_last3months
					, ISNULL(main.days_outOfStock_last12months, 0) AS days_outOfStock_last12months
					, ISNULL(main.nDays_Month, 0) AS nDays_Month
					, ISNULL(main.nDays_priorMonth, 0) AS nDays_priorMonth
					, ISNULL(main.nDays_last3months, 0) AS nDays_last3months
					, ISNULL(main.nDays_last12months, 0) AS nDays_last12months
					, ISNULL(main.nDays_storeTraded_Month, 0) AS nDays_storeTraded_Month
					, ISNULL(main.nDays_storeTraded_priorMonth, 0) AS nDays_storeTraded_priorMonth
					, ISNULL(main.nDays_storeTraded_last3months, 0) AS nDays_storeTraded_last3months
					, ISNULL(main.nDays_storeTraded_last12months, 0) AS nDays_storeTraded_last12months
					, ISNULL(main.stock_on_hand_units_startMonth, 0) AS stock_on_hand_units_startMonth
					, ISNULL(main.stock_on_hand_value_startMonth, 0) AS stock_on_hand_value_startMonth
					, ISNULL(main.stock_on_hand_units_endMonth, 0) AS stock_on_hand_units_endMonth
					, ISNULL(main.stock_on_hand_value_endMonth, 0) AS stock_on_hand_value_endMonth
					, main.md_record_written_timestamp AS md_record_written_timestamp
					, main.md_record_written_pipeline_id AS md_record_written_pipeline_id
					, main.md_transformation_job_id AS md_transformation_job_id
				FROM main
				WHERE incr_date >= DATEADD(MONTH,-@return_last_x_months,getdate());	

				UPDATE STATISTICS [cons_retail].[monthly_store_stock_rate];
			END           

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label='AADCONSINVSKU'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec=max(md_record_written_timestamp) FROM [cons_retail].[monthly_store_stock_rate];
			DELETE FROM [cons_retail].[monthly_store_stock_rate] WHERE md_record_written_timestamp=@newrec;
		END
	END TRY
	BEGIN CATCH
		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'cons_retail.sp_monthly_store_stock_rate' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() AS Updated_date
	END CATCH
END