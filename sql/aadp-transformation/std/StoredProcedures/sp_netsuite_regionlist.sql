SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_netsuite_regionlist] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			DECLARE @read_count [int]
			select @read_count=count(*) from stage.netsuite_regionlist;
			if (@read_count)>0
			BEGIN
				truncate table [std].[netsuite_regionlist]
				
				insert into [std].[netsuite_regionlist]
				select externalid,
				id,
				isinactive,
				name,
				recordid,
				scriptid,
				CAST(CONVERT(DATETIME, [md_record_ingestion_timestamp], 103) AS DATETIME) AS md_record_ingestion_timestamp,
				CAST([md_record_ingestion_pipeline_id] AS VARCHAR(200)) AS [md_record_ingestion_pipeline_id],
				CAST([md_source_system] AS VARCHAR(100)) AS [md_source_system],
				getdate() AS [md_record_written_timestamp],
				@pipelineid AS [md_record_written_pipeline_id],
				@jobid AS [md_transformation_job_id]
				from stage.netsuite_regionlist
				OPTION (LABEL = 'AADPSTDNSRGNLIST');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label varchar(500)
				SET @label='AADPSTDNSRGNLIST'
				EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.netsuite_regionlist;
			
			DELETE FROM std.netsuite_regionlist WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:std.netsuite_regionlist'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_netsuite_regionlist' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO