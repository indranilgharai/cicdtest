/****** Object:  StoredProcedure [std].[sp_fps_person_alias]    Script Date: 3/22/2022 9:02:53 AM ******/
/****** Object:  Modifying StoredProcedure [std].[sp_fps_person_alias]    Script Date: 05/18/2023 7:00:53 AM ******/
/****** Object:  Modifying StoredProcedure [std].[sp_fps_person_alias]    Script Date: 06/21/2023 04:20:53 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_fps_person_alias] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			truncate table std.fps_person_alias;
			
			insert into std.fps_person_alias
			select distinct 
			cast(trim(per.person_uuid) as varchar) as customer_id,
			cast(trim(customer_group_id) as varchar) as customer_group_id,
			cast(trim(home_store) as varchar) as home_store,
			cast(fps_created as date) as create_date,
			cast(fps_last_modified as date) as last_modified,
			cast(trim(email) as varchar(max)) as email
            --, CASE  WHEN   cast(trim(lower(email)) as varchar(max)) LIKE '%@aesop%' THEN 'Y' ELSE 'N' END as is_aesop_employee
			--modified logic to derive is_aesop_employee using workday as source
			,CASE WHEN al.Item_source = 'Workday' THEN 'Y' ELSE 'N' END as is_aesop_employee 
			,getDate() as md_record_written_timestamp
			,@pipelineid as md_record_written_pipeline_id
			,@jobid as md_transformation_job_id
			,'DERIVED' as md_source_system
			from std.fps_person per
			LEFT JOIN std.fps_alias al ON per.person_uuid = al.item_person_uuid
			and al.Item_source = 'Workday'

			OPTION (LABEL = 'AADPSTDFPSPERSN');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDFPSPERSN'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.fps_person_alias;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.fps_person_alias where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_fps_person_alias' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end
