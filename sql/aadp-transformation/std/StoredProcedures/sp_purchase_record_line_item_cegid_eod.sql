/****** Object:  StoredProcedure [std].[sp_purchase_record]    Script Date: 5/20/2022 12:25:32 PM ******/
/****** Modified:  StoredProcedure [std].[sp_purchase_record]    Modified Date: 25/07/2022 12:25:32 PM Modified by: Harsha Varadhi ******/
/****** Modified: Added 'bundle_sku_line_no' and 'bundle_sku_code'    Script Date: 10/10/2023 6:00:00 PM  Modified By: Patrick Lacerna ******/
/****** Modified: Added logic to remove error records from bundle files   Script Date: 10/12/2023 12:30:00 PM  Modified By: Patrick Lacerna ******/
/****** Modified: Added logic to only include 'bundle_sku_line_no' and 'bundle_sku_code' if file is bundle EOD   Script Date: 10/24/2023 09:30:00 AM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_purchase_record_line_item_cegid_eod] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			IF EXISTS (--Checks stage empty or not
					SELECT TOP 1 *
					FROM [stage].[cegid_transactions_store_sales_cegid_temp]
					)
			BEGIN
            --delete the existing common cegid orders from purchase record line item
				DELETE
				FROM std.purchase_record_line_item
				WHERE orderid IN (
						SELECT DISTINCT 'C' + document_internal_reference /* Wiping off the whole order*/
						FROM [stage].[cegid_transactions_store_sales_cegid_temp]
						);

				WITH eod_li
				AS (
					SELECT DISTINCT [orderid]
						,retail_transaction_line_itemid
						,eod.bundle_sku_line_no
						,line_total_net_amount_excl_tax revenue_tax_exc_local
						,line_total_net_amount_excl_tax + line_tax_1_total_amount AS revenue_tax_inc_local
						,CASE 
							WHEN eod.document_currency_code = 'AUD'
								THEN eod.line_total_net_amount_excl_tax
							ELSE (eod.line_total_net_amount_excl_tax / exrate.ex_rate)
							END AS revenue_tax_exc_AUD
						,CASE 
							WHEN eod.document_currency_code = 'AUD'
								THEN eod.line_total_gross_amount_incl_tax
							ELSE (line_total_net_amount_excl_tax + line_tax_1_total_amount / exrate.ex_rate)
							END AS revenue_tax_inc_AUD
						,line_tax_1_total_amount AS tax_amount
						,CASE 
							WHEN eod.document_currency_code = 'AUD'
								THEN eod.line_tax_1_total_amount
							ELSE (eod.line_tax_1_total_amount / exrate.ex_rate)
							END AS tax_amount_AUD
						,eod.[unit_count] AS sales_units
						,eod.line_sales_condition_discount_percentage discount_percentage
						,eod.line_sales_condition_discount_reason_code discount_type
						,eod.total_line_discount_amount discounted_price
						,eod.line_total_gross_amount_excl_tax orig_line_value_pre_discounts
						,eod.item_code product_code
						,eod.bundle_sku_code
						,CASE 
							WHEN eod.document_Date IS NULL
								THEN NULL
							WHEN eod.document_Date = ''
								THEN NULL
							WHEN upper(eod.document_Date) LIKE '%Z'
								THEN convert(DATETIMEOFFSET, eod.document_Date)
							ELSE convert(DATETIMEOFFSET, CONCAT (
										substring(document_date, 1, 4)
										,'-'
										,substring(document_date, 5, 2)
										,'-'
										,substring(document_date, 7, 2)
										,' '
										,substring(document_time, 1, 2)
										,':'
										,substring(document_time, 3, 2)
										,':'
										,substring(document_time, 5, 4)
										))
							END create_date_purchase
						,CASE 
							WHEN unit_count < 0
								THEN 'Y'
							ELSE 'N'
							END AS return_flag
						,CASE 
							WHEN unit_count < 0
								THEN abs(unit_count)
							ELSE 0
							END AS return_qty
						,CASE 
							WHEN unit_count < 0
								THEN cast((abs(unit_count) * abs(line_total_net_amount_excl_tax)) AS FLOAT)
							ELSE 0.000
							END AS return_value
						,CASE 
							WHEN unit_count < 0
								THEN abs(line_tax_1_total_amount)
							ELSE 0.000
							END AS return_value_tax
						,CASE 
							WHEN prd.product_type_cat = 'FINISHED GOOD'
								AND (
									prd.product_type_sub_cat = 'SAMPLE'
									OR prd.product_type_sub_cat = 'PREMIUM SAMPLE'
									)
								THEN 'Y'
							ELSE 'N'
							END AS sample_flag
						,'CEGID' AS source_system
						,eod.md_record_ingestion_timestamp AS ingestion_timestamp
						,getDate() AS md_record_written_timestamp
						,@pipelineid AS md_record_written_pipeline_id
						,@jobid AS md_transformation_job_id
						,'CEGID' AS md_source_system
					FROM (
						SELECT DISTINCT 'C' + document_internal_reference AS [orderid]
							,'C' + document_internal_reference + document_line_number AS retail_transaction_line_itemid
							-- Added logic to only include bundle_sku_line_no if the file is Bundle EOD file
                            ,CASE WHEN body_filename like 'Y2_FFO_BUN_X10%' THEN NULLIF(bundle_sku_line_no, '') ELSE NULL END bundle_sku_line_no
							,document_currency_code
							,cast(replace(line_total_gross_amount_excl_tax,',','') AS FLOAT) line_total_gross_amount_excl_tax
							,cast(replace(line_total_net_amount_excl_tax,',','') AS FLOAT) line_total_net_amount_excl_tax
							,cast(replace(line_total_gross_amount_incl_tax,',','') AS FLOAT) line_total_gross_amount_incl_tax
							,cast(replace(line_tax_1_total_amount,',','') AS FLOAT) line_tax_1_total_amount
							,cast(replace(line_qty,',','') AS FLOAT) AS [unit_count]
							,cast(replace(total_line_discount_amount,',','') AS FLOAT) AS total_line_discount_amount
							,item_code
							-- Added logic to only include bundle_sku_code if the file is Bundle EOD file
                            ,CASE WHEN body_filename like 'Y2_FFO_BUN_X10%' THEN NULLIF(bundle_sku_code,'') ELSE NULL END bundle_sku_code
							,stuff(document_date, 1, patindex('%[0-9]%', document_date) - 1, '') document_Date
							,stuff(document_time, 1, patindex('%[0-9]%', document_time) - 1, '') document_time
							,document_store_code
							,coalesce(line_sales_condition_discount_reason_code, line_manual_discount_reason_code) line_sales_condition_discount_reason_code
							,CASE 
								WHEN cast(replace(line_sales_condition_discount_percentage,',','') AS FLOAT) <> 0.0
									OR line_sales_condition_discount_percentage IS NOT NULL
									THEN cast(replace(line_sales_condition_discount_percentage,',','') AS FLOAT)
								WHEN cast(replace(line_manual_discount_percentage,',','') AS FLOAT) <> 0.0
									OR line_manual_discount_percentage IS NOT NULL
									THEN cast(replace(line_manual_discount_percentage,',','') AS FLOAT)
								ELSE 0.0
								END line_sales_condition_discount_percentage
							,CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) md_record_ingestion_timestamp
							,md_source_system
						FROM [stage].[cegid_transactions_store_sales_cegid_temp]
						WHERE document_internal_reference IS NOT NULL
						AND line_tax_model_code <> '' --Filters out bundle items without proper conversions are excluded [Date Added: 2023-10-12]
						) eod
					LEFT JOIN std.product_X prd ON prd.description1 = eod.item_code
					LEFT JOIN std.store_x st ON cast(eod.document_store_code AS INT) = cast(st.location_code AS INT)
					LEFT JOIN std.subsidiary_x sub ON cast(st.sbs_no AS INT) = cast(sub.sbs_no AS INT)
					LEFT JOIN std.exchange_rate_x exrate ON trim(coalesce(cast(st.sbs_no AS VARCHAR), sub.sbs_no)) = trim(cast(exrate.sbs_no AS VARCHAR))
						AND cast(exrate.year AS INT) = cast(year(CASE 
									WHEN (
											trim(eod.document_Date) IS NULL
											OR trim(eod.document_Date) = ''
											OR trim(eod.document_time) IS NULL
											OR trim(eod.document_time) = ''
											)
										THEN NULL
									WHEN upper(eod.document_Date) LIKE '%Z'
										THEN convert(DATETIMEOFFSET, eod.document_Date)
									ELSE convert(DATETIMEOFFSET, CONCAT (
												substring(document_date, 1, 4)
												,'-'
												,substring(document_date, 5, 2)
												,'-'
												,substring(document_date, 7, 2)
												,' '
												,substring(document_time, 1, 2)
												,':'
												,substring(document_time, 3, 2)
												,':'
												,substring(document_time, 5, 4)
												))
									END) AS INT)
						AND cast(exrate.month_no AS INT) = cast(month(CASE 
									WHEN (
											trim(eod.document_Date) IS NULL
											OR trim(eod.document_Date) = ''
											OR trim(eod.document_time) IS NULL
											OR trim(eod.document_time) = ''
											)
										THEN NULL
									WHEN upper(eod.document_Date) LIKE '%Z'
										THEN convert(DATETIMEOFFSET, eod.document_Date)
									ELSE convert(DATETIMEOFFSET, CONCAT (
												substring(document_date, 1, 4)
												,'-'
												,substring(document_date, 5, 2)
												,'-'
												,substring(document_date, 7, 2)
												,' '
												,substring(document_time, 1, 2)
												,':'
												,substring(document_time, 3, 2)
												,':'
												,substring(document_time, 5, 4)
												))
									END) AS INT)
					)
				INSERT INTO std.purchase_record_line_item
				SELECT [orderid]
					,[retail_transaction_line_itemid]
					,[revenue_tax_exc_local]
					,[revenue_tax_inc_local]
					,[revenue_tax_exc_AUD]
					,[revenue_tax_inc_AUD]
					,[tax_amount]
					,[tax_amount_AUD]
					,[sales_units]
					,[discounted_price]
					,[product_code]
					,NULL AS [product_variant_type]
					,[source_system]
					,[ingestion_timestamp]
					,[create_date_purchase]
					,[return_flag]
					,[return_qty]
					,[return_value]
					,NULL AS return_shipping_flag
					,NULL AS [return_shipping_value]
					,NULL AS [return_date]
					,NULL AS [cancelled_flag]
					,NULL AS [cancellation_qty]
					,NULL AS [cancellation_value]
					,NULL AS [cancellation_shipping_flag]
					,NULL AS [cancellation_shipping_value]
					,NULL AS [cancellation_date]
					,getDate() AS md_record_written_timestamp
					,@pipelineid AS md_record_written_pipeline_id
					,@jobid AS md_transformation_job_id
					,[md_source_system]
					,[sample_flag]
					,NULL AS promotion_code
					,[return_value_tax]
					,NULL AS [cancellation_value_tax]
					,[discount_type]
					,[discount_percentage]
					,[orig_line_value_pre_discounts]
                    ,[bundle_sku_code]
                    ,[bundle_sku_line_no]
				FROM eod_li
				OPTION (LABEL = 'AADPSTDLINEITMEOD');

				PRINT 'AFTER INSERT'

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label VARCHAR(500)

				SET @label = 'AADPSTDLINEITMEOD'

				EXEC meta_ctl.sp_row_count @jobid
					,@step_number
					,@label

				--------------------maintaining history-----------------------
				INSERT INTO std.purchase_record_line_item_history
				SELECT *
				FROM std.purchase_record_line_item;

				--Truncate stage table after std load. This SP should only run after purchase_Record_cegid_eod. This stage table loads purchase_record and purchase_record_line_item tables
				TRUNCATE TABLE stage.[cegid_transactions_store_sales_cegid_temp];

				PRINT 'TRUNCATED STAGE'
			END
			ELSE
			BEGIN
				PRINT 'Stage is Empty'
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME
				,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp)
			FROM std.purchase_record_line_item

			SELECT @onlydate = CAST(@newrec AS DATE);

			DELETE
			FROM std.purchase_record_line_item
			WHERE md_record_written_timestamp = @newrec;

			DELETE
			FROM std.purchase_record_line_item_history
			WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_purchase_record_line_item_cegid_eod' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO
