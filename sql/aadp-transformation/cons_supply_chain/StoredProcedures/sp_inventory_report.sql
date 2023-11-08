/****** Object:  StoredProcedure [cons_supply_chain].[sp_inventory_report]    Script Date: 7/21/2022 3:48:15 AM ******/
/****** Object:  StoredProcedure [cons_supply_chain].[sp_inventory_report]    Modified date Date: 7/21/2022 3:48:15 AM ******/
/**** Modified  StoredProcedure  [Changed the load type from full load to delta load]  Modified Date: 12/08/2022 4:42:21 PM ******/
--updated with no lock to avoid lock and time out issues and updated statistics on table
/**** Modified  StoredProcedure  [Added deleting old records and inserting latest record logic to avoid duplicates]  Modified Date: 24/11/2022 4:42:21 PM ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_supply_chain].[sp_inventory_report] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			TRUNCATE TABLE [cons_supply_chain].[inventory_report_temp];
			DECLARE @latest_observation_date [date]
            select @latest_observation_date=max(observation_date) from [cons_supply_chain].[inventory_report];
			-- added filter to comapre max observation date with soh time stamp for delta load

			With latest_record as (
			select distinct  *,rank() OVER (PARTITION BY item_code, [inventory_number], store_warehouse_code order by soh_time_stamp desc,[md_record_written_timestamp] desc) as rk
			from std.netsuite_item_inventory WITH (NOLOCK) where @latest_observation_date < soh_time_stamp
			),
			product_combined as (
			select  distinct  *,rank() OVER (PARTITION BY product_id order by lastmodifieddate desc,[md_record_written_timestamp] desc) as rk
			from std.netsuite_product_combined WITH (NOLOCK) 
			), 
			location_combined as (
			select distinct *,
			case when coalesce(loc.custrecord1,loc.store_type) in ('3PL','Warehouse','Head Office DC','Head Office') then 'Warehouse'
				when coalesce(loc.custrecord1,loc.store_type) in ('Digital','Online') then 'Digital'
				when coalesce(loc.custrecord1,loc.store_type) in ('Counter','Signature Store','Department Store','Standalone','Shopping Centre') then 'Store'
				when coalesce(loc.custrecord1,loc.store_type) in ('Manufacturing Supplier') then 'Wholesale'
				else '' end as location_category,
				trim(value) as loc_name from std.netsuite_location_combined loc cross apply string_split(fullname,':',1)a where a.ordinal=1
			)
			
			INSERT INTO [cons_supply_chain].[inventory_report_temp]
			select distinct 
			ii.item_code as sku,
			isnull(coalesce(prd.[description],prd.description2),'') as sku_description,
			ii.soh_time_stamp as observation_date,
			isnull(ii.store_warehouse_code,'') as inventory_location,
			coalesce(loc.store_name,loc.loc_name,'')  as inventory_location_name,
			-----updating inventory_location_category as per spec doc, received details from BA----
			loc.location_category as inventory_location_category,
			isnull(ii.store_warehouse_code,'') as location_code,
			coalesce(prd.category,prd.item_mk_category,'') as product_category,
			coalesce(prd.product_type_cat,prd.product_type_category,'') as product_type,
			ii.qty_available as available_to_promise_atp,
			
			case when ii.source='CEGID' then '' 
				 when (ii.source='NETSUITE' and ii.expiry_date>=getdate() and ii.expiry_date< dateadd(month,6,getdate()) ) then 'Y' else 'N' end as stock_approaching_end_of_shelf_life_flag,
			case when ii.source='CEGID' then '' 
				 when (ii.source='NETSUITE' and ii.expiry_date<getdate() ) then 'Y' else 'N' end as end_of_shelf_life_flag,
			ii.[expiry_date] as [expiry_date],
			isnull(ii.inventory_number,'')as batch_number,
			isnull(prd.merge_code,'') as merge_code,
			case when loc.location_category='Store' then (case when ii.physical_inventory<=2 then 'Out of Stock' 
													when ii.physical_inventory<=min_new then 'Low Stock' 
													when ii.physical_inventory>=max_new then 'High Stock' 
													when (ii.physical_inventory>min_new and ii.physical_inventory<max_new) then 'Healthy Stock'end )
				when loc.location_category in ('Warehouse', 'Digital') then (case when ii.physical_inventory<=2 then 'Out of Stock' 
													when ii.physical_inventory<psa.safety_stock then 'Low Stock'
													when ii.physical_inventory>=2*psa.safety_stock then 'High Stock'
													when (ii.physical_inventory>=psa.safety_stock and ii.physical_inventory<2*psa.safety_stock) then 'Healthy Stock'end
												) end as stock_level ,
			coalesce(sbs.sbs_report_region,'') as region,
			coalesce(sbs.sbs_name,'') as subsidiary,
			(ii.qty_in_transit_store+ii.qty_in_transit_warehouse) as inventory_in_transit,
			ii.physical_inventory as stock_on_hand,
			ii.source as source_name,
			ii.stock_status as stock_status,
			prd.averagecost*ii.physical_inventory as stock_on_hand_value,
			prd.averagecost*(ii.qty_in_transit_store+ii.qty_in_transit_warehouse) as stock_in_transit_value,
			case when ii.source='CEGID' then ii.qty_reserved else (ii.physical_inventory-ii.qty_available) end as commit_stock,
			isnull(plc.plc_status,'') as product_life_cycle,
			case when ii.soh_time_stamp=lr.soh_time_stamp then 'Y' else 'N' end as latest_record,
			case when (loc.location_category='Store' and dsr.daily_sales_rate =0 ) then null
				when (loc.location_category='Store' and dsr.daily_sales_rate<>0) then cast(ii.physical_inventory as float)/cast(dsr.daily_sales_rate as float) else null END AS store_coverage,
			case when loc.location_category='Warehouse' then psa.stock_coverage_no_of_weeks_for_soh else null end as warehouse_coverage,
			psa.safety_stock as safety_stock,
			case when loc.location_category='Store' then min_new else null end as min_level,
			case when loc.location_category='Store' then max_new else null end as max_level,
			CONVERT(DATE,psa.forecast_period, 103) as forecast_period,
			CONVERT(DATE,psa.forecast_date, 103) as [Date],
			dt.month_id as Month_Number,
			datepart(week,ii.soh_time_stamp) as week_of_year,
			dt.yearval as year,
			getDate() AS md_record_written_timestamp,
			@pipelineid  AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id
			
			from std.netsuite_item_inventory ii WITH (NOLOCK)
			--added inventory_number in join condition 
			left join (select * from latest_record where rk=1) lr on ii.item_code=lr.item_code and ii.store_warehouse_code=lr.store_warehouse_code
			and ii.inventory_number = lr.inventory_number
			left join (select * from product_combined where rk=1) prd on ii.item_code=prd.product_id
			left join location_combined loc  on ii.store_warehouse_code=loc.location_code
			left join std.netsuite_subsidiary_combined sbs WITH (NOLOCK) on substring(ii.store_warehouse_code,1,2)=sbs.sbs_no
			left join [std].[netsuite_product_life_cycle] plc WITH (NOLOCK) on ii.item_code=plc.item and sbs.country=plc.country_code	
			left join [std].[cegid_replen_min_max] minmax WITH (NOLOCK) on FORMAT(CAST(minmax.sbs_no AS INT),'00','en-US')+FORMAT(CAST(minmax.store_no AS INT),'000','en-US')=ii.store_warehouse_code and minmax.item=ii.item_code 
			left join std.date_dim dt WITH (NOLOCK) on ii.soh_time_stamp=dt.incr_date
			left join (select * from std.fm_product_supply_alerts where eff_to_dt='9999-12-31')psa 
				on psa.sku_code=ii.item_code and psa.location_code=ii.store_warehouse_code 
				and substring(psa.week,1,2)=datepart(week,ii.soh_time_stamp)
				and substring(psa.week,3,4)=cast(year(ii.soh_time_stamp) as varchar)
			left join [std].[netsuite_store_sku_daily_sales_rate] dsr WITH (NOLOCK) on dsr.store_location_code=ii.store_warehouse_code and dsr.item_code=ii.item_code
			--added filter to comapre max observation date with soh time stamp for delta load
			where @latest_observation_date < ii.soh_time_stamp 
			OPTION (LABEL = 'AADCONSINV');

			DECLARE @read_count [int]
			select @read_count=count(*) from [cons_supply_chain].[inventory_report_temp];
			if (@read_count)>0
			BEGIN

			-- no need for truncate and load
            --Truncate table [cons_supply_chain].[inventory_report];
			--deleting old records
			DELETE from [cons_supply_chain].[inventory_report]  where 
			exists (select * from [cons_supply_chain].[inventory_report_temp] temp
			where CAST(CONVERT(DATE,[cons_supply_chain].[inventory_report].observation_date, 103) AS DATE) = CAST(CONVERT(DATE,temp.observation_date, 103) AS DATE)
			and [cons_supply_chain].[inventory_report].sku=temp.sku and [cons_supply_chain].[inventory_report].inventory_location=temp.inventory_location
			and [cons_supply_chain].[inventory_report].stock_status=temp.stock_status)	

            
            INSERT INTO [cons_supply_chain].[inventory_report]
            SELECT * from [cons_supply_chain].[inventory_report_temp];

			Truncate table [cons_supply_chain].[inventory_report_temp];
			UPDATE STATISTICS [cons_supply_chain].[inventory_report];

			END           

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADCONSINV'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [cons_supply_chain].[inventory_report] ;
			
			delete from [cons_supply_chain].[inventory_report] where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'cons_supply_chain.sp_inventory_report' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END