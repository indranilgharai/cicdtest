-- ## SP for load of Standardised table : product_lifecycle_status ##
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_netsuite_product_lifecycle_status] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
		truncate table std.netsuite_product_lifecycle_status;
			insert into [std].[netsuite_product_lifecycle_status]
			select [created],
			[custrecord_ec_plc_priority],
			[custrecord_ec_plc_status_code],
			[custrecord_ec_product_type],
			[externalid],
			[id],
			[isinactive],
			[lastmodified],
			[name],
			[owner],
			[recordid],
			[scriptid],
			getDate() as md_record_written_timestamp,
			@pipelineid as md_record_written_pipeline_id,
			@jobid as md_transformation_job_id
			,'Netsuite'
			from [stage].[netsuite_productlifecyclestatus]
			OPTION (LABEL = 'AADPSTDPRDLFCYL');
	
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDPRDLFCYL'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.netsuite_product_lifecycle_status;
			
			DELETE FROM std.netsuite_product_lifecycle_status WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:std.netsuite_product_lifecycle_status'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_netsuite_product_lifecycle_status' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END