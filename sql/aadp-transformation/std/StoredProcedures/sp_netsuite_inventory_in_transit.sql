SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_inventory_in_transit] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			With [inventory_in_transit] as (
			select distinct *,rank() OVER (PARTITION BY id ORDER BY [md_record_ingestion_timestamp] desc) AS dupcnt
			from [stage].[netsuite_inventoryintransit]
			)
			INSERT INTO [std].[netsuite_inventory_in_transit]
			SELECT  
			 created as created_date
			,custrecord_ec_iit_amount as amount
			,custrecord_ec_iit_batchdetail as batch_detail
			,custrecord_ec_iit_batchrcvd as batch_detail_received
			,custrecord_ec_iit_container as container_detail
			,custrecord_ec_iit_containerrcvd as container_received
			,custrecord_ec_iit_fulfillment_date as fulfillment_date
			,custrecord_ec_iit_intercompany as intercompany
			,custrecord_ec_iit_item as item
			,custrecord_ec_iit_item_fulfillment as item_fulfillment
			,custrecord_ec_iit_last_receipt_date as last_receipt_date
			,custrecord_ec_iit_order as iit_order
			,custrecord_ec_iit_partial_receipts as partial_receipts
			,custrecord_ec_iit_quantity as transit_quantity
			,custrecord_ec_iit_subsidiary as subsidiary
			,externalid as external_id
			,id as id
			,isinactive as is_inactive
			,lastmodified as last_modified
			,[name] as [name]
			,[owner] as owner_employee
			,recordid as record_id
			,scriptid as script_id
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			from (select * from [inventory_in_transit] where dupcnt=1)a	;
			
			IF OBJECT_ID('tempdb..#inventory_in_transit_temp') IS NOT NULL
			BEGIN
				DROP TABLE #inventory_in_transit_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #inventory_in_transit_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select  
				[created_date] ,
				[amount] ,
				[batch_detail] ,
				[batch_detail_received] ,
				[container_detail] ,
				[container_received] ,
				[fulfillment_date] ,
				[intercompany] ,
				[item] ,
				[item_fulfillment] ,
				[last_receipt_date] ,
				[iit_order] ,
				[partial_receipts] ,
				[transit_quantity] ,
				[subsidiary] ,
				[external_id] ,
				[id] ,
				[is_inactive] ,
				[last_modified] ,
				[name] ,
				[owner_employee] ,
				[record_id] ,
				[script_id] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (
				SELECT *, rank() OVER (PARTITION BY id ORDER BY [last_modified] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_inventory_in_transit )a WHERE dupcnt=1 ;

				truncate table std.netsuite_inventory_in_transit;
			
				insert into std.netsuite_inventory_in_transit
				select * from #inventory_in_transit_temp
			OPTION (LABEL = 'AADSTDINVINTRANS');

			DROP TABLE #inventory_in_transit_temp;
			
			UPDATE STATISTICS [std].[netsuite_inventory_in_transit];

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDINVINTRANS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[netsuite_inventory_in_transit];
			
			delete from [std].[netsuite_inventory_in_transit] where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_netsuite_inventory_in_transit' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END