SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_inventory_numbers] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			INSERT INTO [std].[netsuite_inventory_numbers]
			select distinct 
			expirationdate as [expiration_Date]
			,externalid as [external_id]
			,id as [internal_id]
			,item as [item]
			,lastmodifieddate as [last_modified_date]
			,memo as [memo]
			,inventorynumber as [inventory_number_lot]
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			from [stage].[netsuite_inventorynumber];		
			
			IF OBJECT_ID('tempdb..#netsuite_inventory_numbers_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_inventory_numbers_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_inventory_numbers_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [expiration_Date],
				[external_id],
				[internal_id],
				[item],
				[last_modified_date],
				[memo],
				[inventory_number_lot] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (
				SELECT *, rank() OVER (PARTITION BY [internal_id] ORDER BY [last_modified_date] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_inventory_numbers )a WHERE dupcnt=1 ;

			truncate table std.netsuite_inventory_numbers;
			
			insert into std.netsuite_inventory_numbers
			select * from #netsuite_inventory_numbers_temp
			OPTION (LABEL = 'AADSTDINVNUM');

			DROP TABLE #netsuite_inventory_numbers_temp;
			UPDATE STATISTICS std.netsuite_inventory_numbers;
			
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDINVNUM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_inventory_numbers ;
			
			delete from std.netsuite_inventory_numbers where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_netsuite_inventory_numbers' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END