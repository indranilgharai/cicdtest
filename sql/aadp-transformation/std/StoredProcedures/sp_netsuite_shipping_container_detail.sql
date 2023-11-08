SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_shipping_container_detail] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			INSERT INTO std.netsuite_shipping_container_detail
			SELECT DISTINCT 
			custrecord_ec_scd_amount as amount
			,custrecord_ec_scd_batches_received as batches_received
			,custrecord_ec_scd_batches_sent as batches_sent
			,custrecord_ec_scd_batches_origin as batch_origin
			,custrecord_ec_scd_compl_item_receipt as completion_item_receipt
			,created as created_date
			,custrecord_ec_scd_description as [description]
			,externalid as external_id
			,custrecord_ec_scd_gross_amount as gross_amount
			,isinactive as is_inactive
			,id as internal_id
			,custrecord_ec_scd_item as item
			,custrecord_ec_scd_item_fulfillment as item_fulfillment
			,custrecord_ec_scd_item_receipt as item_receipt
			,lastmodified as last_modified_date
			,custrecord_ec_scd_line_number as line_number
			,[name] as [name]
			,custrecord_ec_scd_over_batches as over_batches
			,custrecord_ec_scd_over_quantity as over_quantity
			,custrecord_ec_scd_over_transaction as over_transaction
			,[owner] as [owner]
			,custrecord_ec_scd_price as price
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			FROM [stage].[netsuite_shippingcontainerdetail];

			IF OBJECT_ID('tempdb..#netsuite_shipping_container_detail_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_shipping_container_detail_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_shipping_container_detail_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [amount],
				[batches_received],
				[batches_sent],
				[batch_origin],
				[completion_item_receipt],
				[created_date],
				[description],
				[external_id],
				[gross_amount],
				[is_inactive],
				[internal_id],
				[item],
				[item_fulfillment],
				[item_receipt],
				[last_modified_date],
				[line_number],
				[name],
				[over_batches],
				[over_quantity],
				[over_transaction],
				[owner],
				[price] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (
				SELECT *, rank() OVER (PARTITION BY [internal_id] ORDER BY [last_modified_date] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_shipping_container_detail )a WHERE dupcnt=1 ;

			truncate table std.netsuite_shipping_container_detail;
			
			insert into std.netsuite_shipping_container_detail
			select * from #netsuite_shipping_container_detail_temp
			OPTION (LABEL = 'AADSTDSHPCTRD');

			DROP TABLE  #netsuite_shipping_container_detail_temp;
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDSHPCTRD'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_shipping_container_detail ;
			
			delete from std.netsuite_shipping_container_detail where md_record_written_timestamp=@newrec;
			
		END

		END TRY
		
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_netsuite_shipping_container_detail' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END