SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_ref_store_x_load] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
		IF EXISTS (
					SELECT TOP 1 *
					FROM stage.dwh_store_x
					)
			BEGIN
				TRUNCATE TABLE std.store_x;
				PRINT 'INSIDE LOOP'

			INSERT INTO std.store_x
			SELECT sbs_no
				,store_no
				,netsuite_location
				,store_name
				,pos_terminals
				,exclude
				,city
				,hub_city
				,state
				,sbs_region
				,address1
				,address2
				,postcode
				,phone
				,trading
				,trading_veritas
				,status
				,opening_date
				,closing_date
				,open_date
				,close_date
				,open_months
				,store_or_counter
				,channel
				,store_type
				,store_format
				,location_type
				,floor_space
				,tax_rate
				,total_floor_space
				,counter_type
				,mall_id
				,lfl
				,default_language
				,iso_default_language
				,location_code						
				,getDate() AS md_record_written_timestamp
				,@pipelineid AS md_record_written_pipeline_id
				,@jobid AS md_transformation_job_id
				,'DWH' AS md_source_system
			FROM stage.dwh_store_x
			OPTION (LABEL = 'AADPSTDSTRX');

			UPDATE STATISTICS [std].[store_x];
			
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSTRX'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
TRUNCATE TABLE stage.dwh_store_x;
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
			SELECT @newrec = max(md_record_written_timestamp) FROM std.store_x;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.store_x WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_ref_store_x_load' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
