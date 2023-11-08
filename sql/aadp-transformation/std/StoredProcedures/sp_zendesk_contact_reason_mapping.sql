/* updated SP: 12/06/2023 - updated md_record_ingestion_timestamp and md_record_ingestion_pipeline_id after the stage is made manual load. Updated where condition to include Prior 2021 data */
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_contact_reason_mapping] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
		--Checks whether stage is empty.
			IF EXISTS (
					SELECT TOP 1 *
					FROM [stage].[zendesk_contact_reason_mapping]
					)
			BEGIN
Truncate table  [std].[zendesk_contact_reason_mapping]

insert into [std].[zendesk_contact_reason_mapping]
SELECT distinct
        [ticket_form_id]
        ,[ticket_form_description]
        ,[contact_reason]
        ,[enq_type_description]
        ,[contact_reason_enq_type]
        ,[contact_reason_details1]
        ,[contact_reason_details2]
        ,[Year]
		,getdate() as [md_record_ingestion_timestamp]
		,'manual_load' as [md_record_ingestion_pipeline_id] -- updated for manual load of stage
		,'zendesk' as [md_source_system] -- updated for manual load of stage
		,getdate() as [md_record_written_timestamp]
		,@pipelineid [md_record_written_pipeline_id]
		,@jobid [md_transformation_job_id]
		
  FROM [stage].[zendesk_contact_reason_mapping]
  --where Year ='After_2021' -- Removed where condition to include Prior_2021 also

  	OPTION (LABEL = 'AADPSTDZNDSKCHTS');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKCHTS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table stage.zendesk_contact_reason_mapping
		END
			ELSE
			BEGIN
				PRINT 'Stage is Empty'
			END

		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_contact_reason_mapping;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_contact_reason_mapping where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_zendesk_contact_reason_mapping' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end
GO
