SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_cegid_replen_min_max] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			DECLARE @read_count [int]
			select @read_count=count(*) from stage.dwh_cegid_replen_min_max;
			if (@read_count)>0
			BEGIN			
		
			TRUNCATE TABLE [std].[cegid_replen_min_max]; 

			INSERT INTO [std].[cegid_replen_min_max] 
				SELECT DISTINCT 
				[sbs_no] as [sbs_no],
				[store_no] as [store_no],
				[description1] as [item],
				[min_old] as [min_old],
				[min_new] as [min_new],
				[max_old] as [max_old],
				[max_new] as [max_new],
				[created_user] as [created_user],
				[created_date] as [created_date],
				[export_user] as [export_user],
				[export_date] as [export_date],
				[export_status] as [export_status],
				getdate() as md_record_written_timestamp,
				@pipelineid AS md_record_written_pipeline_id,
				@jobid AS md_transformation_job_id,
				'DWH' as md_source_system 
				from stage.dwh_cegid_replen_min_max
				OPTION (LABEL = 'AADSTDMINMAX');
					--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label varchar(500)
				SET @label='AADSTDMINMAX'
				EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
				
			END
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].cegid_replen_min_max ;
			
			delete from std.cegid_replen_min_max where md_record_written_timestamp=@newrec;
			
		END

		END TRY
		
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_cegid_replen_min_max' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END