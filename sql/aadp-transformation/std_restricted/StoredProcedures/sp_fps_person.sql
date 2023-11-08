/****** Object:  StoredProcedure [std_restricted].[sp_fps_person]    Script Date: 4/8/2022 1:19:41 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std_restricted].[sp_fps_person] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
BEGIN TRY
IF @reset = 0
BEGIN



INSERT INTO std_restricted.fps_person
SELECT distinct [person_uuid],
[dob_1] ,
[first_name_1] ,
[last_name_1] ,
[phone_1] ,
[title_1] ,
[addresses_L_M_title_1] ,
[addresses_L_M_phone_1] ,
[addresses_L_M_line_2_1] ,
[addresses_L_M_line_1] ,
[addresses_L_M_last_name_1] ,
[addresses_L_M_first_name_1] ,
[historical_phones_1] ,
[optins_M_email_L_M_reason_1] ,
[optins_M_email_L_M_last_modified_source] ,
[optins_M_email_L_M_funnel] ,
[optins_M_email_L_M_created] ,
[optins_M_sms_L_M_reason] ,
[optins_M_sms_L_M_last_modified_source] ,
[optins_M_sms_L_M_created] ,
[optins_M_telephonemarketing_L_M_last_modified_source] ,
[optins_M_telephonemarketing_L_M_created] ,
[optins_M_sms_L_M_id] ,
[optins_M_telephonemarketing_L_M_id] ,
[phone_country_code_1] ,
[addresses_L_M_phone_country_code_1] ,
[tax_exemption_code] ,
[email_1] ,
[historical_emails_1] ,
[optins_M_email_L_M_id],
CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as md_record_ingestion_timestamp,
getDate() AS md_record_written_timestamp,
@pipelineid AS md_record_written_pipeline_id,
@jobid AS md_transformation_job_id,
'FPS' AS md_source_system
FROM stage_restricted.fps_person
OPTION (LABEL = 'AADPSTDFPSPRSNR');

UPDATE STATISTICS std_restricted.fps_person;
UPDATE STATISTICS stage_restricted.fps_person;
--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label varchar(500)
SET @label='AADPSTDFPSPRSNR'
EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
END
ELSE
BEGIN
DECLARE @newrec DATETIME, @onlydate DATE
SELECT @newrec = max(md_record_written_timestamp) FROM std_restricted.fps_person;
SELECT @onlydate = CAST(@newrec AS DATE);

DELETE FROM std_restricted.fps_person WHERE md_record_written_timestamp=@newrec;
END
END TRY



BEGIN CATCH
--ERROR OCCURED
PRINT 'ERROR SECTION INSERT'



INSERT meta_audit.transform_error_log_sp
SELECT ERROR_NUMBER() AS ErrorNumber
,ERROR_SEVERITY() AS ErrorSeverity
,ERROR_STATE() AS ErrorState
,'std_restricted.sp_fps_person' AS ErrorProcedure
,ERROR_MESSAGE() AS ErrorMessage
,getdate() AS Updated_date
END CATCH
END
GO


