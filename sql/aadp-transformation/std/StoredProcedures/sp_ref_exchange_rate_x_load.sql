SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_ref_exchange_rate_x_load] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			IF EXISTS (
					SELECT TOP 1 *
					FROM stage.dwh_exchange_rate
					)
			BEGIN
				TRUNCATE TABLE std.exchange_rate_x;
				PRINT 'INSIDE LOOP'

			INSERT INTO std.exchange_rate_x
			SELECT sbs_no
					,month_no
					,year
					,fy
					,ex_rate
					,getDate() AS md_record_written_timestamp
					,@pipelineid AS md_record_written_pipeline_id
					,@jobid AS md_transformation_job_id
					,'DWH' AS md_source_system
			FROM stage.dwh_exchange_rate
			OPTION (LABEL = 'AADPSTDEXRTX');

			UPDATE STATISTICS std.exchange_rate_x;
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDEXRTX'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

	TRUNCATE TABLE stage.dwh_exchange_rate;
				PRINT 'TRUNCATED STAGE'
			END
			ELSE
			BEGIN
				PRINT 'Stage is Empty'
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME
				,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.exchange_rate_x;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.exchange_rate_x WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_ref_exchange_rate_x_load' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
