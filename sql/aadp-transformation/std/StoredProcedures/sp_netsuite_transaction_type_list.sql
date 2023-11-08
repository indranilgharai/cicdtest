SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_transaction_type_list] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			
			INSERT INTO [std].[netsuite_transaction_type_list]
			SELECT DISTINCT 
			externalid as external_id
			,id as id
			,isinactive as is_inactive
			,[name] as [name]
			,recordid as record_id
			,scriptid as script_id
			,[owner] as [owner]
			,created as created_date
			,lastmodified as last_modified_Date
			,custrecord_trantypelist_desc as [description]
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			FROM [stage].[netsuite_transactiontypelist]
			where custrecord_trantypelist_desc is not null;

			IF OBJECT_ID('tempdb..#netsuite_transaction_type_list_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_transaction_type_list_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_transaction_type_list_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [external_id],
					[id],
					[is_inactive],
					[name],
					[record_id],
					[script_id],
					[owner],
					[created_date],
					[last_modified_Date],
					[description],
					[md_record_written_timestamp],
					[md_record_written_pipeline_id],
					[md_transformation_job_id],
					[md_source_system] 
			from (	SELECT *, rank() OVER (PARTITION BY [description] ORDER BY [last_modified_Date] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_transaction_type_list )a WHERE dupcnt=1 ;

				truncate table std.netsuite_transaction_type_list;
			
				insert into std.netsuite_transaction_type_list
				select * from #netsuite_transaction_type_list_temp
				OPTION (LABEL = 'AADSTDTNSTYPLST');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDTNSTYPLST'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_transaction_type_list ;
			
			delete from std.netsuite_transaction_type_list where md_record_written_timestamp=@newrec;
			
		END

		END TRY
		
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_netsuite_transaction_type_list' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END