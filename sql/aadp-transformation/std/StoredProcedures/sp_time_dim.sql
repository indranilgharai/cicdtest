SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_time_dim] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			truncate table std.time_dim 
			insert into std.time_dim 
			select cast(hr_24 as int) hr_24 ,cast(hr_12 as int) hr_12, case when hr_24>8 and hr_24<18 then 'Yes' else 'No' end as business_hour 
			,getDate() as md_record_written_timestamp
			,@pipelineid as md_record_written_pipeline_id
			,@jobid as md_transformation_job_id
			,'DERIVED' as md_source_system
			from (
			select	1	as hr_24,	1	as hr_12	union
			select	2	as hr_24,	2	as hr_13	union
			select	3	as hr_24,	3	as hr_14	union
			select	4	as hr_24,	4	as hr_15	union
			select	5	as hr_24,	5	as hr_16	union
			select	6	as hr_24,	6	as hr_17	union
			select	7	as hr_24,	7	as hr_18	union
			select	8	as hr_24,	8	as hr_19	union
			select	9	as hr_24,	9	as hr_20	union
			select	10	as hr_24,	10	as hr_21	union
			select	11	as hr_24,	11	as hr_22	union
			select	12	as hr_24,	12	as hr_23	union
			select	13	as hr_24,	1	as hr_24	union
			select	14	as hr_24,	2	as hr_25	union
			select	15	as hr_24,	3	as hr_26	union
			select	16	as hr_24,	4	as hr_27	union
			select	17	as hr_24,	5	as hr_28	union
			select	18	as hr_24,	6	as hr_29	union
			select	19	as hr_24,	7	as hr_30	union
			select	20	as hr_24,	8	as hr_31	union
			select	21	as hr_24,	9	as hr_32	union
			select	22	as hr_24,	10	as hr_33	union
			select	23	as hr_24,	11	as hr_34	union
			select	24	as hr_24,	12	as hr_35	 )main
			OPTION (LABEL = 'AADPSTDTIMDIM');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDTIMDIM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.time_dim;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.time_dim where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_time_dim' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end

