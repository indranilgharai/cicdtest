SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_shipping_method] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			DECLARE @read_count [int]
			select @read_count=count(*) from [stage].[netsuite_shippingmethod];
			if (@read_count)>0
			BEGIN

			TRUNCATE TABLE [std].[netsuite_shipping_method]; 

			INSERT INTO  [std].[netsuite_shipping_method]
			select distinct 
			externalid as [external_id]
			,id as [internal_id]
			,isinactive as [is_inactive]
			,[name] as [name]
			,recordid as record_id
			,scriptid as script_id		
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			from [stage].[netsuite_shippingmethod]
			OPTION (LABEL = 'AADSTDSHPMTD');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDSHPMTD'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label	
			
			END
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[netsuite_shipping_method] ;
			
			delete from std.[netsuite_shipping_method] where md_record_written_timestamp=@newrec;
			
		END

		END TRY
		
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_netsuite_shipping_method' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END