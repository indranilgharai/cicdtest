
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std_restricted].[sp_dwh_rp_cust_address] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			insert into [std_restricted].[dwh_rp_cust_address]
			select DISTINCT 
			[cust_sid],
			[phone1],
			[phone2],
			[address1],
			[address2],
			[address3],
			CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp,
			getDate() AS md_record_written_timestamp,
			@pipelineid AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'DWH' AS md_source_system

			from stage_restricted.dwh_rp_cust_address

			OPTION (LABEL = 'AADPSTDDWHRPCADDR');

			UPDATE STATISTICS [std_restricted].[dwh_rp_cust_address];
			
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDDWHRPCADDR'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std_restricted.dwh_rp_cust_address;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std_restricted.dwh_rp_cust_address WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std_restricted.sp_dwh_rp_cust_address' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END

