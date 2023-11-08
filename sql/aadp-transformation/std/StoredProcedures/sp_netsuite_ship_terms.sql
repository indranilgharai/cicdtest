SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_ship_terms] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			INSERT INTO  [std].[netsuite_ship_terms]
			select distinct 
			externalid as external_id
			,custrecord_ec_ship_terms_id as id
			,created as created
			,[name] as [name]
			,[owner] as [owner]
			,recordid as record_id
			,scriptid as script_id
			,lastmodified as last_modified
			,id as internal_id
			,isinactive as is_inactive
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			from [stage].[netsuite_shipterms];

			IF OBJECT_ID('tempdb..#netsuite_ship_terms_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_ship_terms_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_ship_terms_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [external_id],
					[id],
					[created],
					[name],
					[owner],
					[record_id],
					[script_id],
					[last_modified],
					[internal_id]L,
					[is_inactive],
					[md_record_written_timestamp] ,
					[md_record_written_pipeline_id] ,
					[md_transformation_job_id] ,
					[md_source_system] 		
			from (
				SELECT *, rank() OVER (PARTITION BY id ORDER BY [last_modified] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_ship_terms )a WHERE dupcnt=1 ;

				truncate table std.netsuite_ship_terms;
			
				insert into std.netsuite_ship_terms
				select * from #netsuite_ship_terms_temp
			OPTION (LABEL = 'AADSTDSHPTRM');

			DROP TABLE #netsuite_ship_terms_temp;
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDSHPTRM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_ship_terms ;
			
			delete from std.netsuite_ship_terms where md_record_written_timestamp=@newrec;
			
		END

		END TRY
		
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_netsuite_ship_terms' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END

