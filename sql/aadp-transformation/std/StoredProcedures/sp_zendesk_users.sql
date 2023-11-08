/****** Object:  StoredProcedure [std].[sp_zendesk_users]    Script Date: 4/29/2022 6:58:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_users] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

insert into std.zendesk_users
SELECT distinct  [active]
      ,[alias]
      ,[created_at]
      ,[custom_role_id]
      ,[default_group_id]
      ,[details]
      ,[email]
      ,[external_id]
      ,[iana_time_zone]
      ,[id]
      ,[last_login_at]
      ,[locale]
      ,[locale_id]
      ,[moderator]
      ,[name]
      ,[notes]
      ,[only_private_comments]
      ,[organization_id]
      ,[permanently_deleted]
      ,[phone]
      ,[photo]
      ,[report_csv]
      ,[restricted_agent]
      ,[role]
      ,[role_type]
      ,[shared]
      ,[shared_agent]
      ,[shared_phone_number]
      ,[signature]
      ,[suspended]
      ,[tags]
      ,[ticket_restriction]
      ,[time_zone]
      ,[two_factor_auth_enabled]
      ,[updated_at]
      ,[url]
      ,[user_fields]
      ,[verified]
      ,[md_record_ingestion_timestamp]
      ,[md_record_ingestion_pipeline_id]
      ,[md_source_system]
      ,getdate() as [md_record_written_timestamp]
	  ,@pipelineid [md_record_written_pipeline_id]
      ,@jobid [md_transformation_job_id]
  FROM [stage].[zendesk_users]
  

  	OPTION (LABEL = 'AADPSTDZNDSKUSRS');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKUSRS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table [stage].[zendesk_users]

		END

		

		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_users;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_users where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_zendesk_users' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end

  