/****** Object:  StoredProcedure [std].[sp_fps_person_dev]    Script Date: 3/22/2022 7:19:57 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_fps_person] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN			

			INSERT INTO std.fps_person
			SELECT distinct [person_uuid],
			customer_group_id_1 as customer_group_id,
	        [email_1] email,
	        coalesce(home_store,home_store_2) home_store ,
	        cast(fps_created as date) fps_created,
	        cast(fps_last_modified as date) fps_last_modified,
	        CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp,
			getDate() AS md_record_written_timestamp,
			@pipelineid  AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'FPS' AS md_source_system
			FROM stage.fps_person
			OPTION (LABEL = 'AADPSTDFPSPRSN');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDFPSPRSN'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			TRUNCATE TABLE stage.fps_person;
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.fps_person;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.fps_person WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_fps_person' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO