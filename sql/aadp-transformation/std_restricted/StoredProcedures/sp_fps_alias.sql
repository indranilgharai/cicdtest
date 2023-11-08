/****** Object:  StoredProcedure [std_restricted].[sp_fps_alias]    Script Date: 3/31/2022 7:09:24 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std_restricted].[sp_fps_alias] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN			

			INSERT INTO std_restricted.fps_alias
			SELECT distinct  cast([item_person_uuid] as varchar(100)),
	        cast([Item_phone] as varchar(100)),
	        cast([item_email] as varchar(100)),
	        CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp,
			getDate() AS md_record_written_timestamp,
			@pipelineid AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'FPS' AS md_source_system
			FROM stage_restricted.fps_alias
			OPTION (LABEL = 'AADPSTDFPSALSR');
			WITH alias
	                       AS (
				SELECT *
					,rank() OVER (
						PARTITION BY item_person_uuid ORDER BY [md_record_ingestion_timestamp] DESC
							,md_record_written_timestamp DESC
						) AS dupcnt
				FROM std_restricted.fps_alias
				)
			DELETE
			FROM alias
			WHERE dupcnt > 1

			TRUNCATE TABLE stage_restricted.fps_alias;

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDFPSALSR'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label	


		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std_restricted.fps_alias;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std_restricted.fps_alias WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std_restricted.sp_fps_alias' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO

