/****** Object:  StoredProcedure [std].[sp_date_dim]    Script Date: 4/19/2022 9:27:15 AM ******/
/******Modified StoredPocedure [std].[sp_date_dim]
Changes: Hardcoded 2012-01-01 value instead of -11 years current date
Modified date: 8/17/2023 07:00PM Modified by: Ganan Prakash Golakoti******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_date_dim] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			
			truncate table [stage].[number_increment]
			insert into [stage].[number_increment]
			select 0
			union 
			select 1
			union
			select 2
			union
			select 3
			union
			select 4
			union
			select 5
			union
			select 6
			union
			select 7
			union
			select 8
			union
			select 9

			truncate table std.date_dim 
			
			insert into std.date_dim 
			select counterval,trim(cast(year_val as char))+trim(cast(month_id as char))+trim(cast(day_val as char)) date_id,
			cast(incr_date as date) incr_date,
			cast(day_val as int) day_val,
			cast(day_of_week as int) day_of_week,
			trim(day_of_week_string) day_of_week_string,
			trim(business_day) business_day,
			cast(month_id as int) month_id,
			trim(month_name) month_name,
			cast(quarter_no as int) quarter_no,
			trim(quarter_name) quarter_name,
			cast(year_val as int) yearval,
			cast(week_of_year as int) week_of_year,
			getDate() as md_record_written_timestamp,
			@pipelineid as md_record_written_pipeline_id,
			@jobid as md_transformation_job_id,
			'DERIVED' as md_source_system
			from (
			select * from (select counter counterval,getdate() curr_date,DATEADD(day,counter*-1, getdate()) as incr_date,
			day(DATEADD(day,counter*-1, getdate())) day_val,
			DATEPART(WEEKDAY,DATEADD(day,counter*-1, getdate())) day_of_week,
			DATENAME(WEEKDAY,DATEADD(day,counter*-1, getdate())) day_of_week_string,
			case when DATEPART(WEEKDAY,DATEADD(day,counter*-1, getdate())) between 2 and 6 then 'Yes' else 'no' end business_day,
			month(DATEADD(day,counter*-1, getdate())) month_id,
			DATENAME(month,DATEADD(day,counter*-1, getdate())) month_name,
			DATEPART(QUARTER, DATEADD(day,counter*-1, getdate())) quarter_no,
			'Q'+cast(DATEPART(QUARTER, DATEADD(day,counter*-1, getdate())) as char) quarter_name,
			year(DATEADD(day,counter*-1, getdate())) year_val,
			datepart(week,DATEADD(day,counter*-1, getdate())) as week_of_year
			from(
			SELECT
			UNITS.increment + TENS.increment*10 + HUNDREDS.increment*100 + THOUSANDS.increment*1000 counter
			FROM
			stage.number_increment AS UNITS, stage.number_increment AS TENS, stage.number_increment AS HUNDREDS, stage.number_increment AS THOUSANDS
			)a where DATEADD(day,counter*-1, getdate())>=cast('2012-01-01' as date) )b
			
			union
			select * from (
			select counter counterval,getdate() curr_date,DATEADD(day,counter*1, getdate()) as incr_date,
			day(DATEADD(day,counter*1, getdate())) day_val,
			DATEPART(WEEKDAY,DATEADD(day,counter*1, getdate())) day_of_week,
			DATENAME(WEEKDAY,DATEADD(day,counter*1, getdate())) day_of_week_string,
			case when DATEPART(WEEKDAY,DATEADD(day,counter*1, getdate())) between 2 and 6 then 'Yes' else 'no' end business_day,
			month(DATEADD(day,counter*1, getdate())) month_id,
			DATENAME(month,DATEADD(day,counter*1, getdate())) month_name,
			DATEPART(QUARTER, DATEADD(day,counter*1, getdate())) quarter_no,
			'Q'+cast(DATEPART(QUARTER, DATEADD(day,counter*1, getdate())) as char) quarter_name,
			year(DATEADD(day,counter*1, getdate())) year_val,
			datepart(week,DATEADD(day,counter*1, getdate())) as week_of_year
			
			from(
			SELECT
			UNITS.increment + TENS.increment*10 + HUNDREDS.increment*100 + THOUSANDS.increment*1000 counter
			FROM
			stage.number_increment AS UNITS, stage.number_increment AS TENS, stage.number_increment AS HUNDREDS, stage.number_increment AS THOUSANDS
			)a where DATEADD(day,counter*1, getdate())<=DATEADD(year,2, getdate())
			)c
			
			)main
			OPTION (LABEL = 'AADPSTGDATEDIM');

			UPDATE STATISTICS [std].[date_dim];

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTGDATEDIM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.date_dim 
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.date_dim where md_record_written_timestamp=@newrec;
		END
		
	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_date_dim' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
end
GO