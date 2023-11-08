/****** Object:  StoredProcedure [std].[sp_customer_discount_group]    Script Date: 3/22/2022 7:22:11 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_customer_discount_group] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN			

			INSERT INTO std.customer_discount_group
			select distinct cast(trim(customer_code) as [varchar](100)) as  customer_code
			,cast(price_lvl as int) as price_lvl
			,cast(trim(customer_desc) as [varchar](200)) as customer_desc
			,cast(discount_amt as [varchar](10)) as discount_amt
			,cast(trim(country) as [varchar](100)) as country 
			,getdate() as md_record_written_timestamp
			,@pipelineid  AS md_record_written_pipeline_id
			,@jobid  AS md_transformation_job_id
			,'DWH' AS md_source_system
			from stage.dwh_customer_discount_group
			OPTION (LABEL = 'AADPCUSTDGRP');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPCUSTDGRP'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label	
			--TRUNCATING ALL STAGES 
			--EXEC std.sp_stage_truncate
			TRUNCATE TABLE stage.dwh_customer_discount_group;

		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.fps_alias;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM std.customer_discount_group WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_customer_discount_group' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END


GO
