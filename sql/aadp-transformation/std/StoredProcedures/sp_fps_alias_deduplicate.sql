SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- added item_Source as a partition column for staff sale logi change
CREATE PROC [std].[sp_fps_alias_deduplicate] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			WITH alias
			AS (
				SELECT *
					,rank() OVER (
						PARTITION BY item_person_uuid, item_source ORDER BY [md_record_ingestion_timestamp] DESC
							,md_record_written_timestamp DESC
						) AS dupcnt
				FROM std.fps_alias
				)
			DELETE
			FROM alias
			WHERE dupcnt > 1		


			OPTION (LABEL = 'AADPDELALSDUPE');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPDELETEDUPE'

			EXEC meta_ctl.sp_row_count @jobid
				,@step_number
				,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME
				,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp)
			FROM std.fps_person;

			SELECT @onlydate = CAST(@newrec AS DATE);

			DELETE
			FROM std.fps_person
			WHERE md_record_written_timestamp = @newrec;
			
			SELECT @newrec = max(md_record_written_timestamp)
			FROM std.fps_alias;
			
			DELETE
			FROM std.fps_alias
			WHERE md_record_written_timestamp = @newrec;
			
			
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_fps_deduplicate' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END