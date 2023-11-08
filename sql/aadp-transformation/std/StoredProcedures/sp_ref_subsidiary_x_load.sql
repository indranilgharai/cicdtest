SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_ref_subsidiary_x_load] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			IF EXISTS (
					SELECT TOP 1 *
					FROM stage.dwh_subsidiary_x
					)
			BEGIN
				TRUNCATE TABLE std.subsidiary_x;
				PRINT 'INSIDE LOOP'

			INSERT INTO std.subsidiary_x
			SELECT sbs_no
					,sbs_code
					,sbs_code_short
					,sbs_dp_code_short
					,sbs_olympic_code
				   ,sbs_name
				   ,sbs_region
				   ,sbs_report_region
				   ,sbs_currency_code
				   ,sbs_currency_name
				   ,sbs_currency_symbol
				   ,sbs_currency_decimal
				   ,sbs_currency_separator
				   ,gmt_offset
				   ,sbs_order
				   ,sbs_fy_start
				   ,sbs_active
				   ,store_budgets_flag
				   ,support_email
				   ,hybris_site_id
				   ,sbs_warehouse
				   ,sbs_warehouse_code
				,getDate() AS md_record_written_timestamp
				,@pipelineid AS md_record_written_pipeline_id
				,@jobid AS md_transformation_job_id
				,'DWH' AS md_source_system
			FROM stage.dwh_subsidiary_x
			OPTION (LABEL = 'AADPSTDSUBX');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSUBX'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

	TRUNCATE TABLE stage.dwh_subsidiary_x;
				PRINT 'TRUNCATED STAGE'
			END
			ELSE
			BEGIN
				PRINT 'Stage is Empty'
			END
		END
		ELSE
		BEGIN
		DECLARE @newrec DATETIME,@onlydate DATE
		SELECT @newrec = max(md_record_written_timestamp) FROM std.subsidiary_x;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.subsidiary_x WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_ref_subsidiary_x_load' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
