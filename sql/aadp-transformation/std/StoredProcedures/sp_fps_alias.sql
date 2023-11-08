SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
--updated the SP to include source and sourceid
CREATE PROC [std].[sp_fps_alias] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN			

			INSERT INTO std.fps_alias
			SELECT distinct  cast([item_person_uuid] as varchar(100)),
	        cast([item_email] as varchar(100)),
	        cast([Item_fps_last_modified] as datetime),
			cast([Item_source] as varchar(500)),
			cast([Item_source_id] as varchar(500)),
	        CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp,
			getDate() AS md_record_written_timestamp,
			@pipelineid AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'FPS' AS md_source_system
			FROM stage.fps_alias
			OPTION (LABEL = 'AADPSTDFPSALS');
			
			UPDATE STATISTICS std.fps_alias;
			
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDFPSALS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label	

			TRUNCATE TABLE stage.fps_alias;
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.fps_alias;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.fps_alias WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_fps_alias' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END