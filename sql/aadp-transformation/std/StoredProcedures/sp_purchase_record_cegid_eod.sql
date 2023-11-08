/****** Object:  StoredProcedure [std].[sp_purchase_record_cegid_eod]    Script Date: 5/27/2022 6:44:01 AM ******/
/****** Modified: Added logic to derive fields from cegid_online_seller   Script Date: 7/25/2023 9:00:00 AM  Modified By: Patrick Lacerna ******/
/****** Modified: Added logic to remove error records from bundle files   Script Date: 10/12/2023 12:30:00 PM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_purchase_record_cegid_eod] @jobid [int]
	,@step_number [int]
	,@reset [bit]
	,@pipelineid [varchar] (500)
AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
		--Checks whether stage is empty.
			IF EXISTS (
					SELECT TOP 1 *
					FROM [stage].[cegid_transactions_store_sales_cegid_temp]
					)
			BEGIN
			--First,Deletes the existing common cegid orders from purchase_record and later will be reinserted with updated discounted price
				DELETE
				FROM std.purchase_record
				WHERE orderid IN (
						SELECT DISTINCT 'C' + document_internal_reference
						FROM [stage].[cegid_transactions_store_sales_cegid_temp]
						);

				WITH eod_orders
				AS (
					SELECT DISTINCT [orderid]
						,coalesce(eod.customer_fps_code, customer_y2_code) AS [customer_id]
						,eod.customer_y2_code AS source_system_customer_id
						,isnull(st.channel, '') AS [channel_id]
						,sub.sbs_name AS [market_id]
						,st.store_no AS [store_id]
						,sub.sbs_no AS [subsidiary_id]
						,[orderid] AS [purchase_record_id]
						,CASE 
							WHEN eod.document_currency_code = 'AUD'
								THEN eod.document_total_amt_excl_tax
							ELSE (eod.document_total_amt_excl_tax / exrate.ex_rate)
							END AS price
						,eod.document_total_amt_excl_tax AS price_local
						,eod.total_line_discount_amount AS orig_total_discounts_value
						,eod.[unit_count]
						,exrate.ex_rate AS ex_rate
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
						,st.sbs_no AS storx_sbs_no
						,st.store_name AS [store_name]
						,eod.document_currency_code AS currency_code
						,eod.document_store_code AS [fulfillment_location_code]
						,eod.document_store_code AS location_code
						,replace(CASE 
							WHEN charindex('/', coalesce(cashier_code, consultant_code)) >= 1
								THEN substring(coalesce(cashier_code, consultant_code), 1, charindex('/', coalesce(cashier_code, consultant_code)))
							ELSE coalesce(cashier_code, consultant_code)
							END,'/','') sales_consultant
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
							END consignment_status_date
						,eod.md_source_system source_system
						,eod.md_record_ingestion_timestamp AS ingestion_timestamp
						,getDate() AS md_record_written_timestamp
						------------------------metadata fields-------------------
						,@pipelineid AS md_record_written_pipeline_id
						,@jobid AS md_transformation_job_id
						,'CEGID' AS md_source_system
					FROM (
						SELECT DISTINCT 'C' + document_internal_reference AS [orderid]
							,customer_fps_code AS customer_fps_code
							,customer_y2_code customer_y2_code
							,document_currency_code
							,avg(cast(replace(document_total_amt_excl_tax,',','') AS FLOAT)) AS document_total_amt_excl_tax
							,stuff(document_date, 1, patindex('%[0-9]%', document_date) - 1, '') document_Date
							,stuff(document_time, 1, patindex('%[0-9]%', document_time) - 1, '') document_time
							,sum(cast(replace(line_qty,',','') AS FLOAT)) AS [unit_count]
							,document_store_code
							,sum(cast(replace(total_line_discount_amount,',','') AS FLOAT)) AS total_line_discount_amount
							,CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) md_record_ingestion_timestamp
							,md_source_system
							,string_agg(cashier_code, '/') cashier_code
							,string_agg(consultant_code, '/') consultant_code
						FROM (
							SELECT *
							FROM [stage].[cegid_transactions_store_sales_cegid_temp]
							WHERE document_internal_reference IS NOT NULL
							AND line_tax_model_code <> '' --Filters out bundle items without proper conversions are excluded [Date Added: 2023-10-12]
							) a
						GROUP BY 'C' + document_internal_reference
							,customer_fps_code
							,customer_y2_code
							,document_Date
							,document_time
							,document_currency_code
							,document_store_code
							,md_record_ingestion_timestamp
							,md_source_system
						) eod
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
				, eod_orders_2 as (
					SELECT orderid
					,ISNULL(customer_id, '') as customer_id
					,source_system_customer_id
                    ,COALESCE(st.channel, eod.channel_id) as channel_id
					,market_id
					,COALESCE(os.online_store_no, eod.store_id) as store_id
					,subsidiary_id
					,purchase_record_id
					,price
					,price_local
					,ex_rate
					,create_date_purchase
					,COALESCE(st.store_name, eod.store_name) as store_name
					,unit_count
					,ingestion_timestamp
					,source_system
					,NULL AS orderStatus
					,currency_code
					,COALESCE(FORMAT(CAST(os.sbs_no AS INT),'00','en-US')+FORMAT(CAST(os.online_store_no AS INT),'000','en-US'),eod.fulfillment_location_code) as fulfillment_location_code
					,NULL is_gift_card_order
					,COALESCE(FORMAT(CAST(os.sbs_no AS INT),'00','en-US')+FORMAT(CAST(os.online_store_no AS INT),'000','en-US'),eod.location_code) as location_code
                    ,COALESCE(st.sbs_no, eod.storx_sbs_no) as storx_sbs_no
					,consignment_status_date
					,NULL orig_product_discounts_value
					,NULL orig_order_discounts_value
					,orig_total_discounts_value
					,NULL exchange_reference_id_hybris
					,NULL discount_coupon_code
					,NULL promotion_code
					,NULL order_shipping_total
					,NULL order_shipping_total_tax
					,NULL order_type
					,eod.md_record_written_timestamp
					,eod.md_record_written_pipeline_id
					,eod.md_transformation_job_id
					,eod.md_source_system
					,NULL shipped_date
					,sales_consultant
					FROM eod_orders eod
					LEFT JOIN stage.dwh_cegid_online_seller os on replace(eod.sales_consultant,char(13),'') = os.seller_code
					LEFT JOIN std.store_x st ON cast(os.online_store_no AS INT) = cast(st.store_no AS INT) and cast(os.sbs_no AS INT) = cast(st.sbs_no AS INT)
				)
				--Inserting the cegid orders from Cegid EOD table with updated discounted prices/taxes
				INSERT INTO std.purchase_record
				select * from eod_orders_2
				OPTION (LABEL = 'AADSTDPURRECEOD');

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label VARCHAR(500)

				SET @label = 'AADSTDPURRECEOD'

				EXEC meta_ctl.sp_row_count @jobid
					,@step_number
					,@label

				--to delete
				DECLARE @cnt5 INT;

				PRINT 'Final Count'

				SELECT @cnt5 = count(DISTINCT orderid)
				FROM std.purchase_record;

				PRINT cast(@cnt5 AS INT)
				--Purchase_record history insertion
				Insert into std.purchase_record_history
				select * from std.purchase_record;
				PRINT 'Purchase_record history table insertion completed'
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
			FROM std.purchase_record;

			DELETE
			FROM std.purchase_record
			WHERE md_record_written_timestamp = @newrec;

			DELETE
			FROM std.purchase_record_history
			WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH 
		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_purchase_record_cegid_eod' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
