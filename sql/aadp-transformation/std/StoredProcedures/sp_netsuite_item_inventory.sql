/**** Modified  StoredProcedure  [added stock_status to incrase granularity of the table]  Modified Date: 11/08/2022 15:42:21 PM ******/
/**** Modified  StoredProcedure  [included raw material data]  Modified Date: 28/09/2022 15:42:21 PM ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_item_inventory] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
			 
BEGIN
	BEGIN TRY
		IF @reset=0
	    BEGIN

			With [cegid_transactions_store_soh] as (
			select distinct *,rank() OVER (PARTITION BY storeWarehouseCode,itemcode,sohTimeStamp
			ORDER BY [md_record_ingestion_timestamp] desc,body_filename desc,header_method desc,header_correlationid desc) AS dupcnt
			from [stage].[cegid_transactions_store_soh]
			),
			[cegid_transactions_online_soh] as (
			select distinct *,rank() OVER (PARTITION BY storeWarehouseCode,itemcode,[md_record_ingestion_timestamp]
			ORDER BY body_filename desc,header_method desc,header_correlationid desc) AS dupcnt
			from [stage].[cegid_transactions_online_soh]
			),
			[item] as (
			select *,rank() over (partition by id order by [md_record_ingestion_timestamp] desc,lastmodifieddate desc) as dupcnt 
			from stage.netsuite_item 
			),
			[status] as (
			select  distinct location_code,id,trim(a.value) as stock_status from std.netsuite_location_combined cross apply string_split([name],')',1) a where a.ordinal=2
			)
			
			INSERT INTO [std].[netsuite_item_inventory]			
				select distinct 
				 isnull(loc.location_code,'') as store_warehouse_code 
				,it.itemid as item_code 
				,isnull(cast(quantityonhand as float),0) as physical_inventory 
				,isnull(cast(quantityonorder as float),0) as qty_reserved 
				,isnull(cast(quantityavailable as float),0) as qty_available 
				,isnull(cast(quantityintransit as float),0) as qty_in_transit_warehouse 
				,0 as qty_in_transit_store 
				,convert(date,ino.md_record_ingestion_timestamp,103) as soh_time_stamp
				,'Warehouse' as channel
				,'NETSUITE' as source
				,isnull(ino.inventorynumber,'') as inventory_number
				,isnull(CONVERT(DATE, ino.expirationdate,103),'') as expiry_date
				----- this last_modified_date field is used for deduplication process (pick the latest record) and not for reporting purpose-----
				,isnull(ino.lastmodifieddate,'') as last_modified_date
				,case when (sts.stock_status like '%Allocation%' or sts.stock_status like '%Quarantine%' or sts.stock_status like '%Reserved%' or sts.stock_status like '%Donations%' or sts.stock_status like '%Dead Stock%') then sts.stock_status else '' end as stock_status
				,getdate() as md_record_written_timestamp
				,@pipelineid AS md_record_written_pipeline_id
				,@jobid AS md_transformation_job_id
				,'NETSUITE' as md_source_system
				from [stage].[netsuite_inventorynumber] ino 
				left join [stage].[netsuite_inventorylocation] ilo on ino.id=ilo.inventorynumber
				left join std.netsuite_location_combined loc on ilo.location=loc.id
				left join (select distinct * from [item] where dupcnt=1) it on it.id=ino.item		 
				left join [status] sts on ilo.location=sts.id			 
				where it.itemid is not null and loc.location_code != ''
--Added one more union to get raw material data
			 union

			    select distinct
                 isnull(loc.location_code,'') as store_warehouse_code
                ,it.itemid as item_code
                ,isnull(cast(ag.quantityonhand as float),0) as physical_inventory
                ,isnull(cast(ag.quantityonorder as float),0) as qty_reserved
                ,isnull(cast(ag.quantityavailable as float),0) as qty_available
                ,isnull(cast(ag.quantityintransit as float),0) as qty_in_transit_warehouse
                ,0 as qty_in_transit_store
                ,convert(date,ilo.md_record_ingestion_timestamp,103) as soh_time_stamp
                ,'Warehouse' as channel
                ,'NETSUITE' as source
                ,isnull(cast(ilo.inventorynumber as varchar(1000)),'') as inventory_number
                ,null as expiry_date
                ,convert(date,ilo.md_record_ingestion_timestamp,103) as last_modified_date
                ,case when (sts.stock_status like '%Allocation%' or sts.stock_status like '%Quarantine%' or sts.stock_status like '%Reserved%' or sts.stock_status like '%Donations%' or sts.stock_status like '%Dead Stock%') then sts.stock_status else '' end as stock_status
                ,getdate() as md_record_written_timestamp
                ,@pipelineid AS md_record_written_pipeline_id
                ,@jobid AS md_transformation_job_id
                ,'NETSUITE' as md_source_system
                from [stage].[netsuite_inventorybalance] ilo
                left join stage.netsuite_aggregateitemlocation ag on ilo.item = ag.item and ilo.location = ag.location
                left join std.netsuite_location_combined loc on ilo.location=loc.id
                left join (select distinct * from [item] where dupcnt=1) it on it.id=ilo.item        
                left join [status] sts on ilo.location=sts.id            
                where it.itemid is not null and ilo.inventorynumber is null and loc.location_code != ''
			
			 union
			
				select distinct 
				 isnull(trim(storeWarehouseCode),'') as store_warehouse_code 
				,trim(itemCode)  as item_code 
				,isnull(cast(physicalInventory as float),0) as physical_inventory 
				,isnull(cast(qtyReserved as float),0) as qty_reserved 
				,isnull(cast(qtyAvailable as float),0) as qty_available 
				,isnull(cast(qtyInTransitWarehouse as float),0) as qty_in_transit_warehouse 
				,isnull(cast(qtyInTransitStore as float),0) as qty_in_transit_store 				
				, case when (sohTimeStamp is not null or sohTimeStamp<>'')
					then concat(substring(sohTimeStamp,1,4),'-',substring(sohTimeStamp,5,2),'-',substring(sohTimeStamp,7,2))
					else sohTimeStamp end as soh_time_stamp
				,'Retail' as channel
				,'CEGID' as source
				,'' as inventory_number
				,null as expiry_date
				----- this last_modified_date field is used for deduplication process (pick the latest record) and not for reporting purpose-----
				,convert(datetime,concat(substring(sohTimeStamp,1,4),'-',substring(sohTimeStamp,5,2),'-',substring(sohTimeStamp,7,2),' ',substring(sohTimeStamp,9,2),':',substring(sohTimeStamp,11,2),':00')) as last_modified_date
				,'' as stock_status
				,getdate() as md_record_written_timestamp
				,@pipelineid AS md_record_written_pipeline_id
				,@jobid AS md_transformation_job_id
				,'CEGID' as md_source_system 
				from (select distinct * from [cegid_transactions_store_soh] where dupcnt=1 and itemcode is not null) ctss
			
			union 
			
				select distinct 
				 isnull(trim([storeWarehouseCode]),'') as store_warehouse_code 
				,trim([itemCode]) as item_code
				,isnull(cast([physicalInventory] as float),0) as physical_inventory
				,isnull(cast([qtyReserved] as float),0) as qty_reserved
				,isnull(cast([qtyAvailable] as float),0) as qty_available 
				,isnull(cast([qtyInTransitWarehouse] as float),0) as qty_in_transit_warehouse
				,isnull(cast([qtyInTransitStore] as float),0) as qty_in_transit_store
				,convert(date,md_record_ingestion_timestamp,103) as soh_time_stamp
				,'Digital' as channel
				,'CEGID' as source
				,'' as inventory_number
				,null as expiry_date
				----- this last_modified_date field is used for deduplication process (pick the latest record) and not for reporting purpose-----
				,convert(datetime,md_record_ingestion_timestamp,103) as last_modified_date
				,'' as stock_status
				,getdate() as md_record_written_timestamp
				,@pipelineid AS md_record_written_pipeline_id
				,@jobid AS md_transformation_job_id
				,'CEGID' as md_source_system 
				from (select distinct * from [cegid_transactions_online_soh] where dupcnt=1 and itemcode is not null) ctos;

			IF OBJECT_ID('tempdb..#netsuite_item_inventory_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_item_inventory_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_item_inventory_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [store_warehouse_code],
				[item_code],
				[physical_inventory],
				[qty_reserved],
				[qty_available],
				[qty_in_transit_warehouse],
				[qty_in_transit_store],
				[soh_time_stamp],
				[channel],
				[source],
				[inventory_number],
				[expiry_date],
				[last_modified_date],
				[stock_status],
				[md_record_written_timestamp],
				[md_record_written_pipeline_id],
				[md_transformation_job_id],
				[md_source_system]
				--added stock_status to incrase granularity of the table				
			from (
				SELECT *, rank() OVER (PARTITION BY [store_warehouse_code],[item_code], [inventory_number], soh_time_stamp,source,stock_status ORDER BY [last_modified_date] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_item_inventory )a WHERE dupcnt=1 ;

				truncate table std.netsuite_item_inventory;
			
				insert into std.netsuite_item_inventory
				select [store_warehouse_code],
				[item_code],
				sum([physical_inventory]),
				sum([qty_reserved]),
				sum([qty_available]),
				sum([qty_in_transit_warehouse]),
				sum([qty_in_transit_store]),
				[soh_time_stamp],
				[channel],
				[source],
				[inventory_number],
				[expiry_date],
				[last_modified_date],
				[stock_status],
				[md_record_written_timestamp],
				[md_record_written_pipeline_id],
				[md_transformation_job_id],
				[md_source_system]
				from #netsuite_item_inventory_temp
				group by 
				[store_warehouse_code],
				[item_code],
				[soh_time_stamp],
				[channel],
				[source],
				[inventory_number],
				[expiry_date],
				[last_modified_date],
				[stock_status],
				[md_record_written_timestamp],
				[md_record_written_pipeline_id],
				[md_transformation_job_id],
				[md_source_system]
				OPTION (LABEL = 'AADSTDITMINV');

				DROP TABLE #netsuite_item_inventory_temp;
				TRUNCATE TABLE [stage].[cegid_transactions_store_soh];
				TRUNCATE TABLE [stage].[cegid_transactions_online_soh];
				UPDATE STATISTICS std.netsuite_item_inventory;
				UPDATE STATISTICS stage.netsuite_aggregateitemlocation

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDITMINV'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label	
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_item_inventory ;
			
			delete from std.netsuite_item_inventory where md_record_written_timestamp=@newrec;
			
		END
	END TRY
	
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_netsuite_item_inventory' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END