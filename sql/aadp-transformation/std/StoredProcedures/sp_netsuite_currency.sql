-- ## SP for load of Standardised table : netsuite_currency ##
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_netsuite_currency] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			DECLARE @read_count [int]
			select @read_count=count(*) from stage.netsuite_currency;
			if (@read_count)>0
			BEGIN
				truncate table std.netsuite_currency;
				insert into [std].[netsuite_currency]
				select [currencyprecision],
				[displaysymbol],
				[exchangerate],
				[externalid],
				[fxrateupdatetimezone],
				[id],
				[includeinfxrateupdates],
				[isbasecurrency],
				[isinactive],
				[lastmodifieddate],
				[name],
				[overridecurrencyformat],
				[symbol],
				[symbolplacement],
				getDate() as md_record_written_timestamp,
				@pipelineid as md_record_written_pipeline_id,
				@jobid as md_transformation_job_id 
				,'Netsuite'
				from [stage].[netsuite_currency]
				OPTION (LABEL = 'AADPSTDCURR');
		
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label varchar(500)
				SET @label='AADPSTDCURR'
				EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.netsuite_currency;
			
			DELETE FROM std.netsuite_currency WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:std.netsuite_currency'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_netsuite_currency' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END