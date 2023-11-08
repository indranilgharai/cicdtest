/****** Object:  StoredProcedure [cons_supply_chain].[sp_inventory_report_sku]    Script Date: 7/21/2022 3:48:22 AM ******/
/****** Object:  StoredProcedure [cons_supply_chain].[sp_inventory_report]    Modified date Date: 7/21/2022 3:48:15 AM ******/
--updated with no lock to avoid lock and time out issues and updated statistics on table

/****** Object:  StoredProcedure [cons_supply_chain].[sp_inventory_report_sku]    Script Date: 7/21/2022 12:06:31 PM ******/
/****** Modified date Date: 08/17/2022 corrected the table name in update statistics******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_supply_chain].[sp_inventory_report_sku] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			DECLARE @read_count1 [int]
			select @read_count1=count(*) from [cons_supply_chain].[inventory_report];
			if (@read_count1)>0
			BEGIN

				TRUNCATE TABLE [cons_supply_chain].[inventory_report_sku];

				With inventory_report_digital as (
				select distinct  [observation_date],[sku],[inventory_location],[source_name],[stock_status],
								sum([stock_on_hand]) as [stock_on_hand],
								sum([inventory_in_transit]) as [stock_in_transit],
								sum([available_to_promise_atp]) as [available_to_promise_atp],
								sum([commit_stock]) as [commit_stock],
								sum([stock_on_hand_value]) as [stock_on_hand_value], 
								sum([stock_in_transit_value]) as [stock_in_transit_value]
								from [cons_supply_chain].[inventory_report] where inventory_location_category='Digital'
								group by [observation_date],[sku],[inventory_location],[source_name],[stock_status]
				),
				inventory_report_store as (
				select distinct  [observation_date],[sku],[inventory_location],[source_name],[stock_status],
								sum([stock_on_hand]) as [stock_on_hand],
								sum([inventory_in_transit]) as [stock_in_transit],
								sum([available_to_promise_atp]) as [available_to_promise_atp],
								sum([commit_stock]) as [commit_stock],
								sum([stock_on_hand_value]) as [stock_on_hand_value], 
								sum([stock_in_transit_value]) as [stock_in_transit_value]
								from [cons_supply_chain].[inventory_report] where  [inventory_location_category]='Store' and source_name='CEGID'
								group by [observation_date],[sku],[inventory_location],[source_name],[stock_status]
				),
				inventory_report_warehouse as (
				select distinct  [observation_date],[sku],[inventory_location],[source_name],[stock_status],
								sum([stock_on_hand]) as [stock_on_hand],
								sum([inventory_in_transit]) as [stock_in_transit],
								sum([available_to_promise_atp]) as [available_to_promise_atp],
								sum([commit_stock]) as [commit_stock],
								sum([stock_on_hand_value]) as [stock_on_hand_value], 
								sum([stock_in_transit_value]) as [stock_in_transit_value]
								from [cons_supply_chain].[inventory_report] where  [inventory_location_category]='Warehouse' and source_name='NETSUITE'
								group by [observation_date],[sku],[inventory_location],[source_name],[stock_status]
				)
				
				INSERT INTO [cons_supply_chain].[inventory_report_sku]
				
				select distinct 
				st.[observation_date] as [observation_date],
				st.[sku] as [sku],
				ir.[sku_description] as [sku_description],
				ir.[product_category] as [product_category],
				ir.[product_type] as [product_type],	
				ir.[product_life_cycle] as [product_life_cycle],	
				st.[inventory_location] as [inventory_location],
				ir.[inventory_location_name] as [inventory_location_name],
				ir.[inventory_location_category] as [inventory_location_category],
				ir.[region] as [region],
				ir.[subsidiary] as [subsidiary],
				'Available' as [stock_status],
				st.[stock_on_hand] as [stock_on_hand],
				st.[stock_in_transit] as [stock_in_transit],
				st.[available_to_promise_atp] as [available_to_promise_atp],
				st.[commit_stock] as [commit_stock],
				st.[stock_on_hand_value] as [stock_on_hand_value], 
				st.[stock_in_transit_value] as [stock_in_transit_value],	
				case when st.[stock_on_hand]<=2 then 'Out of Stock' 
					when st.[stock_on_hand]<=ir.min_level then 'Low Stock' 
					when st.[stock_on_hand]>=ir.max_level then 'High Stock' 
					when (st.[stock_on_hand]>ir.min_level and st.[stock_on_hand]<ir.max_level) then 'Healthy Stock' end  as [stock_level],
				case when dsr.daily_sales_rate =0 then null else cast(st.[stock_on_hand] as float)/cast(dsr.daily_sales_rate as float) END as [store_coverage],
				null as [warehouse_coverage], 
				null as [safety_stock],
				ir.[min_level] as [min_level], 
				ir.[max_level] as [max_level],
				dsr.sales_units as [sales_units_last_28days],
				dsr.daily_sales_rate as [daily_sales_rate_last_28days],
				ir.[merge_code] as [merge_code],
				ir.[source_name] as [source_name],
				getDate() AS md_record_written_timestamp,
			    @pipelineid   AS md_record_written_pipeline_id,
				@jobid  AS md_transformation_job_id
				from inventory_report_store st WITH (NOLOCK)
				left join (select distinct [observation_date],[sku],[sku_description],[product_category],[product_type],[product_life_cycle],[inventory_location],[inventory_location_name],
				[inventory_location_category],[region],[subsidiary],[stock_status],warehouse_coverage,[safety_stock],[min_level],[max_level],
				[merge_code],[source_name] from [cons_supply_chain].[inventory_report] where  [inventory_location_category]='Store' and source_name='CEGID') ir 
                on  st.[observation_date]=ir.[observation_date] and st.[sku]=ir.sku 
				and st.[inventory_location]=ir.[inventory_location] and st.[stock_status]=ir.[stock_status] and st.source_name=ir.source_name
				left join [std].[netsuite_store_sku_daily_sales_rate] dsr WITH (NOLOCK) on dsr.store_location_code=st.inventory_location and dsr.item_code=st.sku
				
				union 
				
				select distinct 
				wh.[observation_date] as [observation_date],
				wh.[sku] as [sku],
				ir.[sku_description] as [sku_description],
				ir.[product_category] as [product_category],
				ir.[product_type] as [product_type],	
				ir.[product_life_cycle] as [product_life_cycle],	
				wh.[inventory_location] as [inventory_location],
				ir.[inventory_location_name] as [inventory_location_name],
				ir.[inventory_location_category] as [inventory_location_category],
				ir.[region] as [region],
				ir.[subsidiary] as [subsidiary],
				wh.[stock_status] as [stock_status],
				wh.[stock_on_hand] as [stock_on_hand],
				wh.[stock_in_transit] as [stock_in_transit],
				wh.[available_to_promise_atp] as [available_to_promise_atp],
				wh.[commit_stock] as [commit_stock],
				wh.[stock_on_hand_value] as [stock_on_hand_value], 
				wh.[stock_in_transit_value] as [stock_in_transit_value],	
				case when wh.[stock_on_hand]<=2 then 'Out of Stock' 
					when wh.[stock_on_hand]<ir.safety_stock then 'Low Stock'
					when wh.[stock_on_hand]>=2*ir.safety_stock then 'High Stock'
					when (wh.[stock_on_hand]>=ir.safety_stock and wh.[stock_on_hand]<2*ir.safety_stock) then 'Healthy Stock' end as [stock_level],
				null as [store_coverage],
				ir.[warehouse_coverage] as [warehouse_coverage], 
				ir.[safety_stock] as [safety_stock],
				null as [min_level], 
				null as [max_level],
				dsr.sales_units as [sales_units_last_28days],
				dsr.daily_sales_rate as [daily_sales_rate_last_28days],
				ir.[merge_code] as [merge_code],
				ir.[source_name] as [source_name],
				getDate() AS md_record_written_timestamp,
				@pipelineid  AS md_record_written_pipeline_id,
				@jobid  AS md_transformation_job_id
				from inventory_report_warehouse wh 
				left join (select distinct [observation_date],[sku],[sku_description],[product_category],[product_type],[product_life_cycle],[inventory_location],[inventory_location_name],
				[inventory_location_category],[region],[subsidiary],[stock_status],warehouse_coverage,[safety_stock],[min_level],[max_level],
				[merge_code],[source_name] from [cons_supply_chain].[inventory_report]  where  [inventory_location_category]='Warehouse' and source_name='NETSUITE') ir 
                on  wh.[observation_date]=ir.[observation_date] and wh.[sku]=ir.sku 
				and wh.[inventory_location]=ir.[inventory_location] and wh.[stock_status]=ir.[stock_status] and wh.source_name=ir.source_name
				left join [std].[netsuite_store_sku_daily_sales_rate] dsr WITH (NOLOCK) on dsr.store_location_code=wh.inventory_location and dsr.item_code=wh.sku
				
				
				union 
				
				select distinct 
				di.[observation_date] as [observation_date],
				di.[sku] as [sku],
				ir.[sku_description] as [sku_description],
				ir.[product_category] as [product_category],
				ir.[product_type] as [product_type],	
				ir.[product_life_cycle] as [product_life_cycle],	
				di.[inventory_location] as [inventory_location],
				ir.[inventory_location_name] as [inventory_location_name],
				ir.[inventory_location_category] as [inventory_location_category],
				ir.[region] as [region],
				ir.[subsidiary] as [subsidiary],
				'Available' as [stock_status],
				di.[stock_on_hand] as [stock_on_hand],
				di.[stock_in_transit] as [stock_in_transit],
				di.[available_to_promise_atp] as [available_to_promise_atp],
				di.[commit_stock] as [commit_stock],
				di.[stock_on_hand_value] as [stock_on_hand_value], 
				di.[stock_in_transit_value] as [stock_in_transit_value],	
				case when di.source_name='CEGID' then (case when di.[stock_on_hand]<=2 then 'Out of Stock' 
													when di.[stock_on_hand]<=ir.min_level then 'Low Stock' 
													when di.[stock_on_hand]>=ir.max_level then 'High Stock' 
													when (di.[stock_on_hand]>ir.min_level and di.[stock_on_hand]<ir.max_level) then 'Healthy Stock'end )
				when di.source_name='NETSUITE' then (case when di.[stock_on_hand]<=2 then 'Out of Stock' 
													when di.[stock_on_hand]<ir.safety_stock then 'Low Stock'
													when di.[stock_on_hand]>=2*ir.safety_stock then 'High Stock'
													when (di.[stock_on_hand]>=ir.safety_stock and di.[stock_on_hand]<2*ir.safety_stock) then 'Healthy Stock'end
												) end as stock_level ,
				case when dsr.daily_sales_rate =0 then null else cast(di.[stock_on_hand] as float)/cast(dsr.daily_sales_rate as float) END as [store_coverage],
				ir.[warehouse_coverage] as [warehouse_coverage], 
				ir.[safety_stock] as [safety_stock],
				ir.[min_level] as [min_level], 
				ir.[max_level] as [max_level],
				dsr.sales_units as [sales_units_last_28days],
				dsr.daily_sales_rate as [daily_sales_rate_last_28days],
				ir.[merge_code] as [merge_code],
				di.[source_name] as [source_name],
				getDate() AS md_record_written_timestamp,
				@pipelineid  AS md_record_written_pipeline_id,
				@jobid AS md_transformation_job_id
				from (select * from 
				(select *,rank() OVER (PARTITION BY sku,inventory_location,observation_date order by source_name) as rk from inventory_report_digital)a
				where rk=1) di
				left join (select distinct [observation_date],[sku],[sku_description],[product_category],[product_type],[product_life_cycle],[inventory_location],[inventory_location_name],
				[inventory_location_category],[region],[subsidiary],[stock_status],warehouse_coverage,[safety_stock],[min_level],[max_level],
				[merge_code],[source_name] from [cons_supply_chain].[inventory_report] where inventory_location_category='Digital') ir on  di.[observation_date]=ir.[observation_date] and di.[sku]=ir.sku 
				and di.[inventory_location]=ir.[inventory_location] and di.[stock_status]=ir.[stock_status] and di.source_name=ir.source_name
				left join [std].[netsuite_store_sku_daily_sales_rate] dsr WITH (NOLOCK) on dsr.store_location_code=di.inventory_location and dsr.item_code=di.sku

				-- corrected the table name
				UPDATE STATISTICS [cons_supply_chain].[inventory_report_sku];

			END           

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADCONSINVSKU'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [cons_supply_chain].[inventory_report_sku] ;
			
			delete from [cons_supply_chain].[inventory_report_sku] where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'cons_supply_chain.sp_inventory_report_sku' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END