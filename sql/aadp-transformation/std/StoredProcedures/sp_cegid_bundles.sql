SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_cegid_bundles] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			DECLARE @read_count [int]
			select @read_count=count(*) from [stage].[cegid_bundles];
			if (@read_count)>0
			BEGIN			
		
			TRUNCATE TABLE [std].[cegid_bundles]; 

			INSERT INTO [std].[cegid_bundles] 
				SELECT DISTINCT 
				[bundle_code]
                ,[bundle_description]
                ,[bundle_barcode]
                ,[sku_line_no]
                ,[sku_code]
                ,[sku_description]
                ,[sku_barcode]
                ,[sku_qty]
                ,[bundle_date_creation]
                ,[bundle_date_modif]
                ,[bundle_price]
                ,[bundle_currency]
                ,[sku_price]
				,getdate() AS md_record_written_timestamp
				,@pipelineid AS md_record_written_pipeline_id
				,@jobid AS md_transformation_job_id
				,'CEGID' AS md_source_system 
				FROM [stage].[cegid_bundles]
				WHERE [bundle_code] <> ''
				OPTION (LABEL = 'AADSTDBUNDLES');
                
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDBUNDLES'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			TRUNCATE TABLE [stage].[cegid_bundles];
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			SELECT @newrec=max(md_record_written_timestamp) FROM [std].cegid_bundles ;
			DELETE FROM std.cegid_bundles WHERE md_record_written_timestamp=@newrec;
		END

		END TRY
		
	BEGIN CATCH
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_cegid_bundles' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	END CATCH
END