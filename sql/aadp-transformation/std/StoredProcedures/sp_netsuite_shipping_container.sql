SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_shipping_container] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			
			INSERT INTO [std].[netsuite_shipping_container]
			SELECT DISTINCT 
			 custrecord_ec_sc_3PL_date_sent as [3PL_date_sent]
			,custrecord_ec_sc_arrival_date as arrival_date
			,custrecord_ec_sc_billing_address as billing_address
			,created as created
			,custrecord_ec_sc_currency as currency
			,custrecord_ec_sc_eta_date as eta_date
			,custrecord_ec_sc_shipped_date as etd_shipped_date
			,externalid as external_id
			,custrecord_ec_sc_from_address as from_address
			,custrecord_ec_sc_from_location as from_location
			,custrecord_ec_sc_from_subsidiary as from_subsidiary
			,custrecord_ec_sc_from_subsidiary_address as from_subsidiary_address
			,isinactive as is_inactive
			,id as internal_id
			,lastmodified as last_modified_date
			,[name] as [name]
			,custrecord_ec_sc_no_of_cartons as no_of_cartons
			,custrecord_ec_sc_no_of_pallets as no_of_pallets
			,custrecord_ec_sc_order_date as order_date
			,[owner] as [owner]
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			FROM [stage].[netsuite_shippingcontainer];

			IF OBJECT_ID('tempdb..#netsuite_shipping_container_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_shipping_container_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_shipping_container_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [3PL_date_sent],
				[arrival_date],
				[billing_address],
				[created],
				[currency],
				[eta_date],
				[etd_shipped_date],
				[external_id],
				[from_address],
				[from_location],
				[from_subsidiary],
				[from_subsidiary_address],
				[is_inactive],
				[internal_id],
				[last_modified_date],
				[name],
				[no_of_cartons],
				[no_of_pallets],
				[order_date],
				[owner] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (SELECT *, rank() OVER (PARTITION BY [internal_id] ORDER BY [last_modified_date] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_shipping_container )a WHERE dupcnt=1 ;

			truncate table std.netsuite_shipping_container;
			
			insert into std.netsuite_shipping_container
			select * from #netsuite_shipping_container_temp
			OPTION (LABEL = 'AADSTDSHPCTR');

			DROP TABLE #netsuite_shipping_container_temp;
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDSHPCTR'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_shipping_container ;
			
			delete from std.netsuite_shipping_container where md_record_written_timestamp=@newrec;
			
		END

		END TRY
		
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_netsuite_shipping_container' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END