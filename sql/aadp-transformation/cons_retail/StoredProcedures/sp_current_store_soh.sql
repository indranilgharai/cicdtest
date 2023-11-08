/****** Object:  StoredProcedure [cons_retail].[sp_current_store_soh]    Script Date: 12/20/2022 3:38:54 PM ******/
/****** Modified Object:  StoredProcedure [cons_retail].[sp_current_store_soh]    Update Date: 05/09/2023 2:38:54 PM ******/
-- Updated the SP to handle duplicate and changed next order unit caluclation by adding * prd.pack_size
/****** Modified SP: updated Date: 27/06/2023 Modified the CTE for Sales Detail Time to consider sales made
 from digital warehouses which are fulfilled out of virtual warehouses. *******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_current_store_soh] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			
			DECLARE @max_date [date]
			SELECT @max_date=max(soh_time_stamp) from [std].[netsuite_item_inventory]
			
			BEGIN

				TRUNCATE TABLE [cons_retail].[current_store_soh];

				WITH netsuite_item_inventory as ( 
				select  ii.item_code,ii.store_warehouse_code,ii.soh_time_stamp,
						sum(ii.physical_inventory) as physical_inventory,
						sum(ii.qty_in_transit_store) as qty_in_transit_store,
						sum(ii.qty_in_transit_warehouse)  as qty_in_transit_warehouse,
						row_number() over (partition by ii.item_code, isnull(ii.store_warehouse_code,'') order by  ii.soh_time_stamp desc)  as row_num
						from (select isnull(right(store_warehouse_code,5),'') as store_warehouse_code
								,item_code,physical_inventory
								,qty_in_transit_store
								,qty_in_transit_warehouse,soh_time_stamp
							 from [std].[netsuite_item_inventory]
							 where source='CEGID' and cast(soh_time_stamp as date)=@max_date) as ii 	
						group by ii.item_code, ii.store_warehouse_code, ii.soh_time_stamp
				),
				netsuite_product_combined as (
				SELECT T1.*, ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY lastmodifieddate DESC,[md_record_written_timestamp] DESC) AS row_num
					   FROM [std].[netsuite_product_combined] T1
				),
				location_combined as (
				select location_code,
					   case when coalesce(loc.custrecord1,loc.store_type) in ('3PL','Warehouse','Head Office DC','Head Office') then 'Warehouse'
							when coalesce(loc.custrecord1,loc.store_type) in ('Digital','Online') then 'Digital'
							when coalesce(loc.custrecord1,loc.store_type) in ('Counter','Signature Store','Department Store','Standalone','Shopping Centre') then 'Store'
							when coalesce(loc.custrecord1,loc.store_type) in ('Manufacturing Supplier') then 'Wholesale'
						else '' end as location_category,
						trim(value) as loc_name from [std].[netsuite_location_combined] loc cross apply string_split(fullname,':',1) a 
						where a.ordinal=1
				),
				sales_detail_time as (  /* Modified to consider sales made from digital warehouses which are fulfilled out of virtual warehouses */
				select
                                        isnull(
                                                virtual_warehouse_code_location_code
                                                ,case when pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                                      when pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                                      when pr.source_system = 'HYBRIS' THEN CAST(ISNULL(pr.fulfillment_location_code, ISNULL(pr.location_code, '999')) AS VARCHAR(50))
                                                      else NULL
                                                      end
                                                   )as store_fulfillment,
                                                line.product_code AS SKU_number,
                                                cast(
														case
															when (pr.channel_id = 'Digital' )
															then case
																when left(pr.orderid, 1) = 'H' then CASE
																	WHEN pr.source_system = 'HYBRIS'
																	AND (
																		pr.OrderStatus = 'SHIPPED'
																		or pr.OrderStatus = 'DELIVERED'
																		or pr.OrderStatus = 'COMPLETED'
																		or pr.OrderStatus = 'RETURNED'
																	) THEN shipped_date
																	ELSE NULL
																END
																else pr.create_date_purchase
															end
															else pr.create_date_purchase
														end as date
													) AS receipt_date,
                                                case when  line.source_system = 'HYBRIS' and   return_flag='Y' then (abs(sales_units)-return_qty)
                                                         when   line.source_system = 'HYBRIS' and  cancelled_flag='Y' then (abs(sales_units)-cancellation_qty)
                                                         when  line.source_system = 'HYBRIS' and  (return_flag='Y' and cancelled_flag='Y') then (abs(sales_units)-return_qty-cancellation_qty)
                                                        when  line.source_system = 'HYBRIS' then  abs(sales_units)
                                                else sales_units end AS sales_units
                                from [std].[purchase_record] pr
                                                INNER JOIN  [std].[purchase_record_line_item] line
                                                        ON pr.[orderid] = line.[orderid]
                                                LEFT JOIN stage.dwh_digital_locations dl
                                                        on
                                                        case when pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                                                when pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                                                when pr.source_system = 'HYBRIS' THEN CAST(ISNULL(pr.fulfillment_location_code, ISNULL(pr.location_code, '999')) AS VARCHAR(50))
                                                                 else NULL
                                                                 end  = dl.warehouse_code_location_code
                                where line.sample_flag <> 'Y' and line.return_flag <> 'Y' AND pr.[create_date_purchase] BETWEEN DATEADD(month,-3,@max_date) AND @max_date
				),
				inventory_report_sku AS (
				select  ii.item_code as [sku],
						ii.store_warehouse_code as [inventory_location],
						ii.soh_time_stamp as [observation_date],
						sum(ii.physical_inventory) as stock_on_hand
						from (select item_code,isnull(right(store_warehouse_code,5),'') as store_warehouse_code
								,soh_time_stamp,physical_inventory
							from [std].[netsuite_item_inventory]
							where physical_inventory <> 0 AND soh_time_stamp BETWEEN DATEADD(month,-3,@max_date) AND @max_date) as ii
						group by ii.item_code, ii.store_warehouse_code, ii.soh_time_stamp
				), 
				filter_df as (
				select * from
								(
						--to know store is trading
							select  store_fulfillment,
									left([receipt_date], 10) as [receipt_date],
									sum([sales_units]) total_unit_store_level
									from
										sales_detail_time s
									group by
										store_fulfillment,
										left([receipt_date], 10)
									   
									having
										sum([sales_units]) > 0
								) a
								inner join (
							  --   To know sku is in stock
										select  [inventory_location],
												[sku],
												[observation_date],
												stock_on_hand
											from
											   inventory_report_sku inv 
											where stock_on_hand > 0 
																											  
											) b on a.store_fulfillment = b.[inventory_location] and left(a.[receipt_date],10) = b.[observation_date]
											
				),
				 --Get the sales by sku, store and date where it fits the criteria in the first chunk
				 
              
				filter_sales as (                
						select  df.sku as [sku_number],                        
								df.store_fulfillment,                        
								stock_on_hand,                        
								left(df.[receipt_date], 10) as [receipt_date],                        
								isnull(Sum(s.[sales_units]),0) AS total_unit_store_sku_level
								
										from filter_df df
											left join sales_detail_time s 
												on df.store_fulfillment = s.store_fulfillment
												and df.[receipt_date] = left(s.[receipt_date], 10)                                
												and df.[sku] = s.[sku_number]
										group by                            
										df.sku,                            
										df.store_fulfillment,                            
										stock_on_hand,                            
										left(df.[receipt_date], 10)                
										),
				 
				/*filter_sales as (
				select 	s.[sku_number],
						s.store_fulfillment,
						stock_on_hand,
						left(s.[receipt_date], 10) as [receipt_date],
						Sum(s.[sales_units]) AS total_unit_store_sku_level
						from
							sales_detail_time s
							inner join filter_df on filter_df.store_fulfillment = s.store_fulfillment
													and filter_df.[receipt_date] = left(s.[receipt_date], 10)
													and filter_df.[sku] = s.[sku_number]
						 group by
							s.[sku_number],
							s.store_fulfillment,
							stock_on_hand,
							left(s.[receipt_date], 10)
						having
							sum(s.[sales_units]) > 0
				),*/
				 
				--- Rank sales by date in order to identify last 28 days
				ranked_sales as (
				select  [sku_number],
						store_fulfillment,
						[receipt_date],
						stock_on_hand,
						total_unit_store_sku_level,
						dense_rank () over (
							partition by  [sku_number],
							store_fulfillment
							order by
								receipt_date desc
						) date_rank
						from
							filter_sales fs
				),
				 
				--- roll up top 28 days to store/sku level. Also identify the number of days included in roll up, we divide by this number to get the daily rate
				daily_sales_skus as (
				select 	sku_number,
						store_fulfillment,
						sum(total_unit_store_sku_level) as instock_trading_28days_sales,
						max(ranked_sales.date_rank) as ndays_identified
						from
								ranked_sales
						where
								ranked_sales.date_rank < 29
								and sku_number in (
									select
										description1
									from
										[std].[product_x]
									)
								
						group by
							sku_number,
							store_fulfillment
						)
				insert into [cons_retail].[current_store_soh]
				
				select distinct 
				ii.store_warehouse_code locationKey, 
				ii.item_code skuKey,
				concat(ii.store_warehouse_code,ii.item_code) as location_skukey,
				ii.physical_inventory stock_on_hand_units , 
				it.cost * ii.physical_inventory as stock_on_hand_value,
				ii.qty_in_transit_store + ii.qty_in_transit_warehouse as stock_in_transit_units,
				it.cost*(ii.qty_in_transit_store + ii.qty_in_transit_warehouse) as stock_in_transit_value,
				vm.vm_min as vm_minimum_units,
				it.cost * vm.vm_min as vm_minimum_value,
				case when ii.physical_inventory - ISNULL(vm.vm_min,0) > 0 
						 then ii.physical_inventory - ISNULL(vm.vm_min, 0) else 0  
				end	as sellable_soh_units,
				case when (( ISNULL(it.cost, 0) * ISNULL(ii.physical_inventory, 0)) - (ISNULL(it.cost,0) * ISNULL(vm.vm_min,0))) > 0 
						then ( ISNULL(it.cost, 0) * ISNULL(ii.physical_inventory, 0)) - (ISNULL(it.cost,0) * ISNULL(vm.vm_min,0)) else 0 
				end	as sellable_soh_value ,		 
				case when loc.location_category='Store' 
						then minmax.min_new else null 
				end as cegid_min,
				case when loc.location_category='Store' 
						then minmax.max_new else null 
				end as cegid_max,
				case when ii.physical_inventory < (case when loc.location_category='Store' 
																then minmax.min_new else null end)
							then case when (cast((case when loc.location_category='Store' 
															then minmax.max_new else null end) as int) % cast(prd.pack_size as int)) > 0.4
											then ceiling((case when loc.location_category='Store' then minmax.max_new else null end) / prd.pack_size) 
									   else floor((case when loc.location_category='Store' then minmax.max_new else null end) / prd.pack_size) 
								 end 
				end  as expected_next_order_packs, 
				case when ii.physical_inventory < (case when loc.location_category='Store' 
														then minmax.min_new else null end)
							then case when (cast((case when loc.location_category='Store' 
															then minmax.max_new else null end) as int) % cast(prd.pack_size as int)) > 0.4
											then ceiling((case when loc.location_category='Store' 
																	then minmax.max_new else null end) / prd.pack_size) * prd.pack_size
											else floor((case when loc.location_category='Store' 
																   then minmax.max_new else null end) / prd.pack_size) * prd.pack_size 
								  end 
				end as expected_next_order_units,
				sales.instock_trading_28days_sales as instock_trading_28days_sales_units, 
				sales.ndays_identified as ndays_identified,
				round(cast(sales.instock_trading_28days_sales as float)/cast(sales.ndays_identified as float),2) as daily_sales_rate, 
				ii.soh_time_stamp as soh_time_stamp,
				getDate() AS md_record_written_timestamp,
			    @pipelineid   AS md_record_written_pipeline_id,
				@jobid  AS md_transformation_job_id
				from netsuite_item_inventory ii
				left join netsuite_product_combined prd on ii.item_code=prd.product_id and prd.row_num = 1 
				left join location_combined loc  on ii.store_warehouse_code = loc.location_code
				left join [std].[dimitem_location] it on it.sku_code = ii.item_code and it.locationid = ii.store_warehouse_code and it.active_record = 1
				left join [std].[cegid_replen_min_max] minmax WITH (NOLOCK) on (ii.store_warehouse_code = format(cast(minmax.sbs_no as int),'00','en-us')+ 											 format(cast(minmax.store_no as int),'000','en-us')
																		 and ii.item_code = minmax.item)
				left join [std].[store_inventory_vm_min] vm on substring(ii.store_warehouse_code,3,3) = format (cast(vm.store_no as int),'000','en-us') 
														   and substring(ii.store_warehouse_code,1,2) = format (cast(vm.sbs_no as int),'00','en-us') 
															and ii.item_code = vm.description1
				left join daily_sales_skus sales on sales.sku_number = ii.item_code and sales.store_fulfillment = ii.store_warehouse_code
				where ii.row_num = 1

				-- corrected the table name
				UPDATE STATISTICS [cons_retail].[current_store_soh];

			END           

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADCONSINVSKU'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [cons_retail].[current_store_soh] ;
			
			delete from [cons_retail].[current_store_soh] where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'cons_retail.sp_current_store_soh' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END
