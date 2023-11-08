-- ## SP for load of Standardised table : CLASS ##
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_netsuite_class] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			DECLARE @read_count [int]
			select @read_count=count(*) from stage.netsuite_class;
			if (@read_count)>0
			BEGIN
				truncate table [std].[netsuite_class];
				insert into [std].[netsuite_class]
				select [custrecord_ec_class_capex_appr_limit],
				[custrecord_ec_class_capex_approver],
				[custrecord_ec_class_code],
				[custrecord_ec_class_id],
				[custrecord_ec_class_opex_appr_limit],
				[custrecord_ec_class_opex_approver],
				[custrecord_ec_class_veritas_available],
				[externalid],
				[fullname],
				[id],
				[includechildren],
				[isinactive],
				[lastmodifieddate],
				[name],
				[parent],
				[subsidiary],
				getDate() as md_record_written_timestamp,
				@pipelineid as md_record_written_pipeline_id,
				@jobid as md_transformation_job_id,
				'Netsuite'
				from stage.netsuite_class
				OPTION (LABEL = 'AADPSTDCLASS');
				
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label varchar(500)
				SET @label='AADPSTDCLASS'
				EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.netsuite_class;
			
			DELETE FROM std.netsuite_class WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:std.netsuite_class'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_netsuite_class' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END