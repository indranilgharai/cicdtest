SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_product_life_cycle] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			WITH [item] as (
			select distinct *,rank() over (partition by id order by [md_record_ingestion_timestamp] desc,lastmodifieddate desc) as dupcnt 
			from stage.netsuite_item 
			)
			INSERT INTO [std].[netsuite_product_life_cycle]
			SELECT DISTINCT
			it.itemid as item
			,plc.custrecord_ec_plc_date_available as available_date
			,plc.custrecord_ec_plc_region as country
			,plc.custrecord_ec_plc_country_code as country_code
			,plc.created as created
			,plc.custrecord_ec_plc_date_discountinued as discontinued_date
			,plc.externalid as external_id
			,plc.lastmodified as last_modified
			,plc.[name] as [name]
			,plc.custrecord_ec_plc_node_attribute as [node]
			,plc.custrecord_ec_plc_obsolete_date as obsolete_Date
			,plc.[owner] as [owner]
			,plc.custrecord_ec_plc_date_phaseout as phase_out_date
			,plc.scriptid as script_id
			,plcs.[name] as plc_status 
			,plc.id as id
			,plc.isinactive as is_inactive
			,plc.altname as alt_name
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			from [stage].[netsuite_productlifecycle] plc
			left join (select * from [item] where dupcnt=1) it on plc.custrecord_ec_plc_item=it.id
			left join [stage].[netsuite_productlifecyclestatus] plcs on plc.custrecord_ec_plc_status=plcs.id;

			IF OBJECT_ID('tempdb..#netsuite_product_life_cycle_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_product_life_cycle_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_product_life_cycle_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [item],
				[available_date],
				[country],
				[country_code],
				[created],
				[discontinued_date],
				[external_id],
				[last_modified],
				[name],
				[node],
				[obsolete_date],
				[owner],
				[phase_out_date],
				[script_id],
				[plc_status],
				[id],
				[is_inactive],
				[alt_name] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (
				SELECT *, rank() OVER (PARTITION BY [item],[country_code] ORDER BY [last_modified] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_product_life_cycle )a WHERE dupcnt=1 ;

			truncate table std.netsuite_product_life_cycle;
			
			insert into std.netsuite_product_life_cycle
			select * from #netsuite_product_life_cycle_temp
			OPTION (LABEL = 'AADSTDPLC');

			DROP TABLE #netsuite_product_life_cycle_temp;
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDPLC'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[netsuite_product_life_cycle];
			
			delete from std.netsuite_product_life_cycle where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_netsuite_product_life_cycle' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END
