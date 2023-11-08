SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [meta_audit].[sp_tfm_wrapper] @jobid [int],@reset [bit],@pipelineid [nvarchar](1000) AS
Begin
	BEGIN TRY
		IF EXISTS(SELECT a.name FROM sys.tables a WHERE a.name like 'rbstep') 
		BEGIN
			DROP TABLE meta_ctl.rbstep;
		END;
		IF EXISTS(SELECT b.name FROM sys.tables b WHERE b.name like 'rbrunstep') 
		BEGIN
			DROP TABLE meta_ctl.rbrunstep;
		END;
		IF EXISTS(SELECT c.name FROM sys.tables c WHERE c.name like 'rerunstep') 
		BEGIN
			DROP TABLE meta_ctl.rerunstep;
		END;
		IF EXISTS(SELECT d.name FROM sys.tables d WHERE d.name like 'tjsteps') 
		BEGIN
			DROP TABLE meta_ctl.tjsteps;
		END;
		DECLARE @last_jobrun_time datetime
		select @last_jobrun_time=max(job_end_time) from meta_audit.transform_job_stats where job_id=@jobid and job_status='SUCCESS';
		PRINT 'last_jobrun_time: ';
		PRINT 'last_jobrun_time: ';
		PRINT @last_jobrun_time;
		IF @last_jobrun_time is NULL
			BEGIN
				SET @last_jobrun_time='1990-01-01 11:27:41.477'
			END
		IF EXISTS(
		SELECT * FROM meta_audit.transform_job_stats 
		where job_id=@jobid and job_start_time>=@last_jobrun_time and job_status='FAIL'
		)
		BEGIN
		PRINT 'Inside the rerun block';
			If (@reset=1)
			BEGIN
				-- Going to rollback all the inserts that have run successully in the last run for the current job.
			
				create table meta_ctl.rbstep
				with (
				DISTRIBUTION = ROUND_ROBIN
				)
				AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, step_number FROM meta_audit.transform_job_step_stats where job_id=@jobid and step_status='SUCCESS' and step_start_time>@last_jobrun_time;
				
				DECLARE @rocounter int = 1
				DECLARE @rorecordCount int = (SELECT COUNT(*) from meta_ctl.rbstep)
				WHILE @rocounter <= @rorecordCount 
				BEGIN 
					DECLARE @rb_step INT = (SELECT a.step_number FROM meta_ctl.rbstep a where a.Sequence = @rocounter);
					DECLARE @rb_name nvarchar(100), @rb_parms nvarchar(1000), @rbdependent_step_numbers nvarchar(100)
					DECLARE @reflag nvarchar(100)
					SET @reflag='@reset=1'
					DECLARE @pipeid1 varchar(500)
					SET @pipeid1=(SELECT @pipelineid)
					SELECT @rb_name=sp_name, @rb_parms=sp_parms, @rbdependent_step_numbers=dependent_step_num FROM meta_ctl.transform_job_steps where job_id=@jobid and step_number=@rb_step
					-- if 'reset' flag=1, then it should search for those fields having max date value in column : md_record_written_timestamp and delete only those columns - this will delete the newly added column.
					DECLARE @rblast_step_time datetime
					SELECT @rblast_step_time=max(step_end_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_number=@rb_step and step_status='SUCCESS'
					IF @rb_parms is null
					BEGIN
						DECLARE @rbsql nvarchar(1000)=(SELECT @rb_name)+' '+(SELECT @reflag)+','+'@pipelineid='+(SELECT @pipeid1)
						PRINT @rbsql 
						EXEC sp_executesql @rbsql
					END
					ELSE
					BEGIN
						DECLARE @rbsql1 nvarchar(1000)=(SELECT @rb_name)+' '+(SELECT @reflag)+','+(SELECT @rb_parms)+','+'@pipelineid='+(SELECT @pipeid1)
						PRINT @rbsql1 
						EXEC sp_executesql @rbsql1
					END
					
					Update meta_audit.transform_job_step_stats SET step_start_time=null,step_end_time=null,step_status='FAIL',log_message=null,pipeline_id=null
					where job_id=@jobid and step_number=@rb_step and step_end_time=@rblast_step_time and step_status='SUCCESS';
					SET @rocounter = @rocounter + 1
				END
				DROP TABLE meta_ctl.rbstep;
				create table meta_ctl.rbrunstep
				with (
				DISTRIBUTION = ROUND_ROBIN
				)
				AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, step_number FROM meta_audit.transform_job_step_stats where job_id=@jobid and (step_status is NULL or step_status='FAIL');
				
				DECLARE @rrbcounter int = 1
				DECLARE @rrbrecordCount int = (SELECT COUNT(*) from meta_ctl.rbrunstep)
				
				DECLARE	@rrblast_step_time datetime 
				
				WHILE @rrbcounter <= @rrbrecordCount 
				BEGIN 
					PRINT 'Inside while loop';
					DECLARE @rrbrun_step INT = (SELECT a.step_number FROM meta_ctl.rbrunstep a where a.step_number = @rrbcounter);
					DECLARE @rrbsp_name nvarchar(100), @rrbsp_parms nvarchar(1000), @rrbdependent_step_numbers nvarchar(100)
					DECLARE @rrbflag nvarchar(100)
					SET @rrbflag='@reset=0' --This will run the actual insert script in the sp instead of passing the control to the rollback script block.
					DECLARE @pipeid2 varchar(500)
					SET @pipeid2=(SELECT @pipelineid)
					SELECT @rrbsp_name=sp_name, @rrbsp_parms=sp_parms, @rrbdependent_step_numbers=dependent_step_num FROM meta_ctl.transform_job_steps where job_id=@jobid and step_number=@rrbrun_step
					DECLARE @rrbstep_status nvarchar(100) ='SUCCESS'
					DECLARE	@rrblog_message nvarchar(4000) = 'SP completed successfully'
					DECLARE @rrbstep_status1 nvarchar(100) ='SUCCESS'
					DECLARE	@rrblog_message1 nvarchar(4000) = 'SP completed successfully'
					SELECT @rrblast_step_time=max(step_end_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_number=@rrbrun_step				
					IF @rrblast_step_time is NULL
					BEGIN
						SET @rrblast_step_time='1990-01-01 11:27:41.477'
					END
					PRINT @rrblast_step_time
					PRINT 'This SP is dependent on the below SPs:'
					PRINT @rrbdependent_step_numbers
					IF @rrbdependent_step_numbers is NULL
					BEGIN
						PRINT 'has no dependent SPs'
						declare @rrbstepstart datetime = getdate();
						PRINT @rrbstepstart
					-- if 'reset' flag=1, then it should search for those fields having max date value in column : md_record_written_timestamp and delete only those columns - this will delete the newly added column.
						IF @rrbsp_parms is null
						BEGIN
							DECLARE @rrbsql nvarchar(1000)=(SELECT @rrbsp_name)+' '+(SELECT @rrbflag)+','+'@pipelineid='+(SELECT @pipeid2)
							PRINT @rrbsql 
							EXEC sp_executesql @rrbsql
						END
						ELSE
						BEGIN
							DECLARE @rrbsql1 nvarchar(1000)=(SELECT @rrbsp_name)+' '+(SELECT @rrbflag)+','+(SELECT @rrbsp_parms)+','+'@pipelineid='+(SELECT @pipeid2)
							PRINT @rrbsql1 
							EXEC sp_executesql @rrbsql1
						END
					
						PRINT 'Execution completed'
						declare @rrbstepend datetime = getdate();
						PRINT @rrbstepend
							
									
						IF EXISTS(SELECT * FROM meta_audit.transform_error_log_sp where ErrorProcedure=@rrbsp_name and updated_date>=@rrbstepstart)
						BEGIN
							SET @rrbstep_status='FAIL';
							SELECT @rrblog_message=ErrorMessage from meta_audit.transform_error_log_sp where ErrorProcedure=@rrbsp_name and Updated_date>=@rrbstepstart;
						END

						PRINT @rrbstep_status
						PRINT @rrblog_message
						DECLARE @driver_read_count int=0
						DECLARE @target_write_count int=0
						DECLARE @latest_val datetime
						SELECT @latest_val=max(md_record_written_timestamp) from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@rrbrun_step;
						SELECT @driver_read_count=driver_read_count,@target_write_count=target_write_count from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@rrbrun_step and md_record_written_timestamp>=@rrbstepstart;
					--Updating/inserting into fourth meta table -- this will remove the previous Failure entry and update it with the success run record values
						IF EXISTS (SELECT * FROM meta_audit.transform_job_step_stats where job_id=@jobid and step_number=@rrbrun_step and step_status='FAIL')
						BEGIN
							PRINT 'going to update fourth meta table'
						--Update the fourth meta table
							Update meta_audit.transform_job_step_stats SET step_start_time=@rrbstepstart,step_end_time=@rrbstepend,step_status=@rrbstep_status,log_message=@rrblog_message,driver_read_count=@driver_read_count,target_write_count=@target_write_count,pipeline_id=@pipelineid
							where job_id=@jobid and step_number=@rrbrun_step and step_status='FAIL'
						END
						ELSE
						BEGIN			
					--Insertion of fourth meta table
						
							PRINT 'going to insert fourth meta table'
							Insert meta_audit.transform_job_step_stats values (@jobid,@rrbrun_step,@rrbstepstart,@rrbstepend,@rrbstep_status,@rrblog_message,@driver_read_count,@target_write_count,@pipelineid);
						END
					END
					ELSE
					BEGIN
						PRINT 'has dependent SPs'
						IF EXISTS(SELECT [name] FROM sys.tables WHERE [name] like 'dep1') 
						BEGIN
							DROP TABLE meta_ctl.dep1;
						END;
						IF EXISTS(SELECT [name] FROM sys.tables WHERE [name] like 'temp_dep_status1') 
						BEGIN
							DROP TABLE meta_ctl.temp_dep_status1;
						END;

						create table meta_ctl.dep1
						with (
						DISTRIBUTION = ROUND_ROBIN
						)
						AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, value FROM STRING_SPLIT(@rrbdependent_step_numbers, ',');
						DECLARE @depcounter1 int = 1
						DECLARE @deprecordCount1 int = (SELECT COUNT(*) from meta_ctl.dep1)
						DECLARE @depstatus1 varchar(40)
						
						DECLARE @depdate1 datetime = (SELECT CAST(getdate() as date))
						CREATE TABLE [meta_ctl].[temp_dep_status1]
						(
							[step_status] [varchar](40) NULL
						)
						WITH
						(
						DISTRIBUTION = ROUND_ROBIN,
						CLUSTERED COLUMNSTORE INDEX
						)
						WHILE @depcounter1 <= @deprecordCount1 
						BEGIN 
							PRINT 'Going to check the status of dependent SPs'
							DECLARE @dep_sp1 INT = (SELECT a.value FROM meta_ctl.dep1 a where a.Sequence = @depcounter1);
							select @depstatus1=step_status from [meta_audit].[transform_job_step_stats] 
							where step_number=@dep_sp1 and job_id=@jobid and step_start_time>@last_jobrun_time
							Insert meta_ctl.temp_dep_status1 values (@depstatus1)
							SET @depcounter1 = @depcounter1 + 1
						END			
						IF (select TOP 1 step_status from meta_ctl.temp_dep_status1 where step_status='FAIL')= 'FAIL'
						BEGIN
							PRINT 'FAILURE in DEPENDENT SP'
							Insert meta_audit.transform_error_log_sp
								  SELECT ERROR_NUMBER() AS ErrorNumber ,
								  ERROR_SEVERITY() AS ErrorSeverity ,
								  ERROR_STATE() AS ErrorState ,
								  @rrbsp_name AS ErrorProcedure ,
								  'FAILURE in DEPENDENT SP' AS ErrorMessage,
								  getdate() as Updated_date
						END
						ELSE
						BEGIN
							PRINT 'ALL DEPENDENT SPs SUCCESSFUL'
							
							declare @rrbstepstart1 datetime = getdate();
							IF @rrbsp_parms is null
							BEGIN
								DECLARE @rrbsql2 nvarchar(1000)=(SELECT @rrbsp_name)+' '+(SELECT @rrbflag)+','+'@pipelineid='+(SELECT @pipeid2)
								PRINT @rrbsql2
								EXEC sp_executesql @rrbsql2
							END
							ELSE
							BEGIN
								DECLARE @rrbsql3 nvarchar(1000)=(SELECT @rrbsp_name)+' '+(SELECT @rrbflag)+','+(SELECT @rrbsp_parms)+','+'@pipelineid='+(SELECT @pipeid2)
								PRINT @rrbsql3 
								EXEC sp_executesql @rrbsql3
							END
							declare @rrbstepend1 datetime = getdate();	
							IF EXISTS(SELECT * FROM meta_audit.transform_error_log_sp where ErrorProcedure=@rrbsp_name and updated_date>=@rrbstepstart1)
							BEGIN
								SET @rrbstep_status1='FAIL';
								SELECT @rrblog_message1=ErrorMessage from meta_audit.transform_error_log_sp where ErrorProcedure=@rrbsp_name and Updated_date>=@rrbstepstart1;
							END
							DECLARE @driver_read_count2 int=0
							DECLARE @target_write_count2 int=0
							SELECT @driver_read_count2=driver_read_count,@target_write_count2=target_write_count from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@rrbrun_step and md_record_written_timestamp>=@rrbstepstart1;		
							--Insertion of fourth meta table
							PRINT 'into not rerun loop - fourth meta table'
							--Updating/inserting into fourth meta table -- this will remove the previous Failure entry and update it with the success run record values
							IF EXISTS (SELECT * FROM meta_audit.transform_job_step_stats where job_id=@jobid and step_number=@rrbrun_step and step_status='FAIL')
							BEGIN
								PRINT 'going to update fourth meta table'
							--Update the fourth meta table
								Update meta_audit.transform_job_step_stats SET step_start_time=@rrbstepstart1,step_end_time=@rrbstepend1,step_status=@rrbstep_status1,log_message=@rrblog_message1,driver_read_count=@driver_read_count2,target_write_count=@target_write_count2,pipeline_id=@pipelineid
								where job_id=@jobid and step_number=@rrbrun_step and step_status='FAIL'
							END
							ELSE
							BEGIN			
						--Insertion of fourth meta table
						
								PRINT 'going to insert fourth meta table'
								Insert meta_audit.transform_job_step_stats values (@jobid,@rrbrun_step,@rrbstepstart1,@rrbstepend1,@rrbstep_status1,@rrblog_message1,@driver_read_count2,@target_write_count2,@pipelineid);
							END

						END
					END

				SET @rrbcounter = @rrbcounter + 1
				END
				DROP TABLE meta_ctl.rbrunstep;
				DECLARE @rrbjobstart datetime
				DECLARE @rrbjobend datetime
				DECLARE @rrbjoblog varchar(4000) = 'Job completed successfully'
				DECLARE @rrbjob_status varchar(100) ='SUCCESS'

				SELECT @rrbjobstart=MIN(step_start_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_start_time>@last_jobrun_time
				SELECT @rrbjobend=MAX(step_end_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_end_time>@last_jobrun_time

				IF EXISTS (SELECT * FROM meta_audit.transform_job_step_stats where step_status='FAIL' and job_id=@jobid)
				BEGIN
					SET @rrbjob_status='FAIL';
					SET @rrbjoblog='Failure occured at related SP';
				END

				PRINT @rrbjob_status;
				PRINT @rrbjoblog;

				--Updating third meta table
				IF EXISTS (SELECT * FROM meta_audit.transform_job_stats where job_id=@jobid and job_start_time>@last_jobrun_time and job_status='FAIL')
				BEGIN
					PRINT 'Updating 3rd meta table'
					Update meta_audit.transform_job_stats SET job_start_time=@rrbjobstart, job_end_time=@rrbjobend,job_status=@rrbjob_status,log_message=@rrbjoblog,pipeline_id=@pipelineid
					WHERE job_id=@jobid and  job_start_time>=@last_jobrun_time and job_status='FAIL'
				END	
			END
			ELSE
			BEGIN
				PRINT 'RERUN BLOCK WITH RESET=0'
				create table meta_ctl.rerunstep
				with (
				DISTRIBUTION = ROUND_ROBIN
				)
				AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, step_number FROM meta_audit.transform_job_step_stats where job_id=@jobid and (step_status is NULL or step_status='FAIL');
				SELECT min(step_number) from meta_ctl.rerunstep;
				PRINT 'table meta_ctl.rerunstep created with rerun SPs'
				DECLARE @recounter int = 1
				DECLARE @rerecordCount int = (SELECT COUNT(*) from meta_ctl.rerunstep)
				
				DECLARE	@relast_step_time datetime 
				DECLARE @nxtrunsp int=0;
				DECLARE @lastrunsp int=0;
				PRINT 'Going into while loop'
				WHILE @recounter <= @rerecordCount 
				BEGIN 
					PRINT 'Inside while loop'
					PRINT 'SP that was last rerun:'
					PRINT @lastrunsp
					SELECT @nxtrunsp=min(step_number) FROM meta_audit.transform_job_step_stats where job_id=@jobid and (step_status is NULL or step_status='FAIL') and step_number>@lastrunsp;
					SET @lastrunsp=@nxtrunsp
					PRINT 'SP that is going to be rerun now:'
					PRINT @nxtrunsp
					PRINT 'Inside while loop of rerun block with reset=0'
					DECLARE @rerun_step INT = (SELECT a.step_number FROM meta_ctl.rerunstep a where a.step_number = @nxtrunsp);
					DECLARE @resp_name nvarchar(100), @resp_parms nvarchar(1000), @redependent_step_numbers nvarchar(100)
					DECLARE @rflag nvarchar(100)
					SET @rflag='@reset=0'
					DECLARE @pipeid3 varchar(500)
					SET @pipeid3=(SELECT @pipelineid)
					SELECT @resp_name=sp_name, @resp_parms=sp_parms, @redependent_step_numbers=dependent_step_num FROM meta_ctl.transform_job_steps where job_id=@jobid and step_number=@rerun_step
					DECLARE @restep_status nvarchar(100) ='SUCCESS'
					DECLARE	@relog_message nvarchar(4000) = 'SP completed successfully'
					DECLARE @restep_status1 nvarchar(100) ='SUCCESS'
					DECLARE	@relog_message1 nvarchar(4000) = 'SP completed successfully'
					PRINT 'This SP is dependent on the below SPs:'
					PRINT @redependent_step_numbers
					IF @redependent_step_numbers is NULL
					BEGIN
						PRINT 'has no dependent SPs'
						declare @restepstart datetime = getdate();
						PRINT @restepstart
					-- if 'reset' flag=1, then it should search for those fields having max date value in column : md_record_written_timestamp and delete only those columns - this will delete the newly added column.
						IF @resp_parms is null
						BEGIN
							DECLARE @rsql nvarchar(1000)=(SELECT @resp_name)+' '+(SELECT @rflag)+','+'@pipelineid='+(SELECT @pipeid3)
							PRINT @rsql 
							EXEC sp_executesql @rsql
						END
						ELSE
						BEGIN
							DECLARE @rsql1 nvarchar(1000)=(SELECT @resp_name)+' '+(SELECT @rflag)+','+(SELECT @resp_parms)+','+'@pipelineid='+(SELECT @pipeid3)
							PRINT @rsql1 
							EXEC sp_executesql @rsql1
						END
					--EXEC sp_executesql @resp_name, @resp_parms;
						PRINT 'Execution completed'
						declare @restepend datetime = getdate();
						PRINT @restepend
							
									
						IF EXISTS(SELECT * FROM meta_audit.transform_error_log_sp where ErrorProcedure=@resp_name and updated_date>=@restepstart)
						BEGIN
							SET @restep_status='FAIL';
							SELECT @relog_message=ErrorMessage from meta_audit.transform_error_log_sp where ErrorProcedure=@resp_name and Updated_date>=@restepstart;
						END

						PRINT @restep_status
						PRINT @relog_message
						DECLARE @driver_read_count1  int=0
						DECLARE @target_write_count1 int=0
						DECLARE @latest_val1 datetime
						SELECT @latest_val1=max(md_record_written_timestamp) from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@rerun_step;
						SELECT @driver_read_count1 =driver_read_count,@target_write_count1=target_write_count from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@rerun_step and md_record_written_timestamp>=@restepstart;
					--Updating/inserting into fourth meta table -- this will remove the previous Failure entry and update it with the success run record values
						IF EXISTS (SELECT * FROM meta_audit.transform_job_step_stats where job_id=@jobid and step_number=@rerun_step and step_status='FAIL')
						BEGIN
							PRINT 'going to update fourth meta table'
						--Update the fourth meta table
							Update meta_audit.transform_job_step_stats SET step_start_time=@restepstart,step_end_time=@restepend,step_status=@restep_status,log_message=@relog_message,driver_read_count=@driver_read_count1 ,target_write_count=@target_write_count1,pipeline_id=@pipelineid
							where job_id=@jobid and step_number=@rerun_step and step_status='FAIL'
						END
						ELSE
						BEGIN			
					--Insertion of fourth meta table
						
							PRINT 'going to insert fourth meta table'
							Insert meta_audit.transform_job_step_stats values (@jobid,@rerun_step,@restepstart,@restepend,@restep_status,@relog_message,@driver_read_count1 ,@target_write_count1,@pipelineid);
						END
					END
					ELSE
					BEGIN
						IF EXISTS(SELECT [name] FROM sys.tables WHERE [name] like 'dep2') 
						BEGIN
							DROP TABLE meta_ctl.dep2;
						END;
						IF EXISTS(SELECT [name] FROM sys.tables WHERE [name] like 'temp_dep_status2') 
						BEGIN
							DROP TABLE meta_ctl.temp_dep_status2;
						END;

						create table meta_ctl.dep2
						with (
						DISTRIBUTION = ROUND_ROBIN
						)
						AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, value FROM STRING_SPLIT(@redependent_step_numbers, ',');
						DECLARE @depcounter2 int = 1
						DECLARE @deprecordCount2 int = (SELECT COUNT(*) from meta_ctl.dep2)
						DECLARE @depstatus2 varchar(40)
						
						DECLARE @depdate2 datetime = (SELECT CAST(getdate() as date))
						CREATE TABLE [meta_ctl].[temp_dep_status2]
						(
							[step_status] [varchar](40) NULL
						)
						WITH
						(
						DISTRIBUTION = ROUND_ROBIN,
						CLUSTERED COLUMNSTORE INDEX
						)
						WHILE @depcounter2 <= @deprecordCount2 
						BEGIN 
							DECLARE @dep_sp2 INT = (SELECT a.value FROM meta_ctl.dep2 a where a.Sequence = @depcounter2);
							select @depstatus2=step_status from [meta_audit].[transform_job_step_stats] 
							where step_number=@dep_sp2 and job_id=@jobid and step_start_time>@last_jobrun_time
							Insert meta_ctl.temp_dep_status2 values (@depstatus2)
							SET @depcounter2 = @depcounter2 + 1
						END			
						IF (select TOP 1 step_status from meta_ctl.temp_dep_status2 where step_status='FAIL')= 'FAIL'
						BEGIN
							PRINT 'FAILURE in DEPENDENT SP'
							Insert meta_audit.transform_error_log_sp
								  SELECT ERROR_NUMBER() AS ErrorNumber ,
								  ERROR_SEVERITY() AS ErrorSeverity ,
								  ERROR_STATE() AS ErrorState ,
								  @resp_name AS ErrorProcedure ,
								  'FAILURE in DEPENDENT SP' AS ErrorMessage,
								  getdate() as Updated_date
						END
						ELSE
						BEGIN
							PRINT 'ALL DEPENDENT SPs SUCCESSFUL'
							--EXEC sp_executesql @resp_name, @resp_parms;
							declare @restepstart1 datetime = getdate();
							IF @resp_parms is null
							BEGIN
								DECLARE @rsql2 nvarchar(1000)=(SELECT @resp_name)+' '+(SELECT @rflag)+','+'@pipelineid='+(SELECT @pipeid3)
								PRINT @rsql2
								EXEC sp_executesql @rsql2
							END
							ELSE
							BEGIN
								DECLARE @rsql3 nvarchar(1000)=(SELECT @resp_name)+' '+(SELECT @rflag)+','+(SELECT @resp_parms)+','+'@pipelineid='+(SELECT @pipeid3)
								PRINT @rsql3 
								EXEC sp_executesql @rsql3
							END
							declare @restepend1 datetime = getdate();	
							IF EXISTS(SELECT * FROM meta_audit.transform_error_log_sp where ErrorProcedure=@resp_name and updated_date>=@restepstart1)
							BEGIN
								SET @restep_status1='FAIL';
								SELECT @relog_message1=ErrorMessage from meta_audit.transform_error_log_sp where ErrorProcedure=@resp_name and Updated_date>=@restepstart1;
							END
							DECLARE @driver_read_count12 int=0
							DECLARE @target_write_count12 int=0
							SELECT @driver_read_count12=driver_read_count,@target_write_count12=target_write_count from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@rerun_step and md_record_written_timestamp>=@restepstart1		
							--Insertion of fourth meta table
							PRINT 'into not rerun loop - fourth meta table'
							--Updating/inserting into fourth meta table -- this will remove the previous Failure entry and update it with the success run record values
							IF EXISTS (SELECT * FROM meta_audit.transform_job_step_stats where job_id=@jobid and step_number=@rerun_step and step_status='FAIL')
							BEGIN
								PRINT 'going to update fourth meta table'
							--Update the fourth meta table
								Update meta_audit.transform_job_step_stats SET step_start_time=@restepstart1,step_end_time=@restepend1,step_status=@restep_status1,log_message=@relog_message1,driver_read_count=@driver_read_count12 ,target_write_count=@target_write_count12,pipeline_id=@pipelineid
								where job_id=@jobid and step_number=@rerun_step and step_status='FAIL'
							END
							ELSE
							BEGIN			
							--Insertion of fourth meta table
						
								PRINT 'going to insert fourth meta table'
								Insert meta_audit.transform_job_step_stats values (@jobid,@rerun_step,@restepstart1,@restepend1,@restep_status1,@relog_message1,@driver_read_count12 ,@target_write_count12,@pipelineid);
							END
						END
					
					END
					SET @recounter = @recounter + 1
				END
				DROP TABLE meta_ctl.rerunstep;
				DECLARE @rejobstart datetime
				DECLARE @rejobend datetime
				DECLARE @rejoblog varchar(4000) = 'Job completed successfully'
				DECLARE @rejob_status varchar(100) ='SUCCESS'

				SELECT @rejobstart=MIN(step_start_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_start_time>@last_jobrun_time
				SELECT @rejobend=MAX(step_end_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_end_time>@last_jobrun_time

				IF EXISTS (SELECT * FROM meta_audit.transform_job_step_stats where step_status='FAIL' and job_id=@jobid)
				BEGIN
					SET @rejob_status='FAIL';
					SET @rejoblog='Failure occured at related SP';
				END

				PRINT @rejob_status;
				PRINT @rejoblog;

				--Updating third meta table
				IF EXISTS (SELECT * FROM meta_audit.transform_job_stats where job_id=@jobid and job_start_time>@last_jobrun_time and job_status='FAIL')
				BEGIN
					PRINT 'Updating 3rd meta table'
					Update meta_audit.transform_job_stats SET job_start_time=@rejobstart, job_end_time=@rejobend,job_status=@rejob_status,log_message=@rejoblog,pipeline_id=@pipelineid
					WHERE job_id=@jobid and  job_start_time>=@last_jobrun_time and job_status='FAIL'
				END	
			END
		END
		ELSE
		BEGIN
			-- First time run block
			IF EXISTS(SELECT * FROM meta_ctl.transform_job_master WHERE job_id=@jobid) 
			BEGIN
				create table meta_ctl.tjsteps
				with (
				DISTRIBUTION = ROUND_ROBIN
				)
				AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, step_number, sp_name, sp_parms, dependent_step_num FROM meta_ctl.transform_job_steps where job_id=@jobid;

				DECLARE @counter int = 1
				DECLARE @recordCount int = (SELECT COUNT(*) from meta_ctl.tjsteps)
				
				DECLARE	@last_step_time datetime 

				-- Loop through records in Temp table
				WHILE @counter <= @recordCount 
				BEGIN  
					  DECLARE @step_number INT = (SELECT a.step_number FROM meta_ctl.tjsteps a where a.step_number = @counter);
					  DECLARE @sp_name NVARCHAR(4000)= (SELECT a.sp_name FROM meta_ctl.tjsteps a where a.step_number = @counter);
					  DECLARE @sp_parms NVARCHAR(500) = (SELECT a.sp_parms FROM meta_ctl.tjsteps a where a.step_number = @counter);
					  DECLARE @dependent_step_num VARCHAR(500) = (SELECT a.dependent_step_num FROM meta_ctl.tjsteps a where a.step_number = @counter);
					  DECLARE @fflag nvarchar(100)
					  SET @fflag='@reset=0'
					  DECLARE @pipeid varchar(500)
					  SET @pipeid=(SELECT @pipelineid)
					  PRINT 'JOB ID is:'
					  PRINT @jobid
					  SELECT @last_step_time=max(step_end_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_number=@step_number
					  
					  DECLARE @step_status nvarchar(100) ='SUCCESS'
					  DECLARE	@log_message nvarchar(4000) = 'SP completed successfully'
					  DECLARE @step_status1 nvarchar(100) ='SUCCESS'
					  DECLARE	@log_message1 nvarchar(4000) = 'SP completed successfully'
					  IF @last_step_time is NULL
					  BEGIN
						SET @last_step_time='1990-01-01 11:27:41.477'
					  END
					  PRINT @last_step_time
					  PRINT 'STEP NUMBER:'
					  PRINT @step_number
					  PRINT '@dependent_step_num:'
					  PRINT @dependent_step_num

					  --check if the sp has a dependent sp and if it has run successfully or not
					  IF @dependent_step_num is null
					  BEGIN
					  PRINT 'No dependent SPs'
					  --EXEC sp_executesql @sp_name, @sp_parms;
							declare @stepstart datetime = getdate();
							IF @sp_parms is null
							BEGIN
								PRINT 'Going to exec'
								PRINT 'sp_name:'
								PRINT @sp_name
								PRINT 'fflag'
								PRINT @fflag
								PRINT '@pipeid:'
								PRINT @pipeid
								DECLARE @fsql nvarchar(1000)=(SELECT @sp_name)+' '+(SELECT @fflag)+','+'@pipelineid='+(SELECT @pipeid)
								PRINT @fsql 
								EXEC sp_executesql @fsql
							END
							ELSE
							BEGIN
								PRINT 'GOING TO EXECUTE THE SP'
								DECLARE @fsql1 nvarchar(1000)=(SELECT @sp_name)+' '+(SELECT @fflag)+','+(SELECT @sp_parms)+','+'@pipelineid='+(SELECT @pipeid)
								PRINT @fsql1 
								EXEC sp_executesql @fsql1
							END
							declare @stepend datetime = getdate();	
							IF EXISTS(SELECT * FROM meta_audit.transform_error_log_sp where ErrorProcedure=@sp_name and updated_date>=@stepstart)
								BEGIN
									SET @step_status='FAIL';
									SELECT @log_message=ErrorMessage from meta_audit.transform_error_log_sp where ErrorProcedure=@sp_name and Updated_date>=@stepstart;
								END
							DECLARE @driver_read_count5 int=0
							DECLARE @target_write_count5 int=0
							SELECT @driver_read_count5=driver_read_count,@target_write_count5=target_write_count from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@step_number and md_record_written_timestamp>@stepstart;		
							--Insertion of fourth meta table
							PRINT ' Insertion of fourth meta table: meta_audit.transform_job_step_stats'
							Insert meta_audit.transform_job_step_stats values (@jobid,@step_number,@stepstart,@stepend,@step_status,@log_message,@driver_read_count5,@target_write_count5,@pipelineid);

					  END
					  ELSE
					  BEGIN
							IF EXISTS(SELECT [name] FROM sys.tables WHERE [name] like 'dep') 
							BEGIN
								DROP TABLE meta_ctl.dep;
							END;
							IF EXISTS(SELECT [name] FROM sys.tables WHERE [name] like 'temp_dep_status') 
							BEGIN
								DROP TABLE meta_ctl.temp_dep_status;
							END;

							create table meta_ctl.dep
							with (
							DISTRIBUTION = ROUND_ROBIN
							)
							AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, value FROM STRING_SPLIT(@dependent_step_num, ',');
							DECLARE @depcounter int = 1
							DECLARE @deprecordCount int = (SELECT COUNT(*) from meta_ctl.dep)
							DECLARE @depstatus varchar(40)
							
							DECLARE @depdate datetime = (SELECT CAST(getdate() as date))
							CREATE TABLE [meta_ctl].[temp_dep_status]
							(
								[step_status] [varchar](40) NULL
							)
							WITH
							(
							DISTRIBUTION = ROUND_ROBIN,
							CLUSTERED COLUMNSTORE INDEX
							)
							WHILE @depcounter <= @deprecordCount 
							BEGIN 
								DECLARE @dep_sp INT = (SELECT a.value FROM meta_ctl.dep a where a.Sequence = @depcounter);
								select @depstatus=step_status from [meta_audit].[transform_job_step_stats] 
								where step_number=@dep_sp and job_id=@jobid and step_start_time>@last_jobrun_time
								Insert meta_ctl.temp_dep_status values (@depstatus)
								SET @depcounter = @depcounter + 1
							END			
							IF (select TOP 1 step_status from meta_ctl.temp_dep_status where step_status='FAIL')= 'FAIL'
							BEGIN
								PRINT 'FAILURE in DEPENDENT SP'
								Insert meta_audit.transform_error_log_sp
									  SELECT ERROR_NUMBER() AS ErrorNumber ,
									  ERROR_SEVERITY() AS ErrorSeverity ,
									  ERROR_STATE() AS ErrorState ,
									  @sp_name AS ErrorProcedure ,
									  'FAILURE in DEPENDENT SP' AS ErrorMessage,
									  getdate() as Updated_date
							END
							ELSE
							BEGIN
								PRINT 'ALL DEPENDENT SPs SUCCESSFUL'
								
								declare @stepstart1 datetime = getdate();
								IF @sp_parms is null
								BEGIN
									DECLARE @fsql2 nvarchar(1000)=(SELECT @sp_name)+' '+(SELECT @fflag)+','+'@pipelineid='+(SELECT @pipeid)
									PRINT @fsql2
									EXEC sp_executesql @fsql2
								END
								ELSE
								BEGIN
									DECLARE @fsql3 nvarchar(1000)=(SELECT @sp_name)+' '+(SELECT @fflag)+','+(SELECT @sp_parms)+','+'@pipelineid='+(SELECT @pipeid)
									PRINT @fsql3 
									EXEC sp_executesql @fsql3
								END
								declare @stepend1 datetime = getdate();	
								IF EXISTS(SELECT * FROM meta_audit.transform_error_log_sp where ErrorProcedure=@sp_name and updated_date>=@stepstart1)
								BEGIN
									SET @step_status1='FAIL';
									SELECT @log_message1=ErrorMessage from meta_audit.transform_error_log_sp where ErrorProcedure=@sp_name and Updated_date>=@stepstart1;
								END
								DECLARE @driver_read_count3 int=0
								DECLARE @target_write_count3 int=0
								SELECT @driver_read_count3=driver_read_count,@target_write_count3=target_write_count from meta_ctl.[transform_count_record_table] where job_id=@jobid and step_number=@step_number and md_record_written_timestamp>@stepstart1;		
								--Insertion of fourth meta table
								PRINT 'Inserting value into fourth meta table meta_audit.transform_job_step_stats'
								
								Insert meta_audit.transform_job_step_stats values (@jobid,@step_number,@stepstart1,@stepend1,@step_status1,@log_message1,@driver_read_count3,@target_write_count3,@pipelineid);

							END
					  END
									
					  
					  SET @counter = @counter + 1
				END
				DROP TABLE meta_ctl.tjsteps;
				DECLARE @jobstart datetime
				DECLARE @jobend datetime
				DECLARE @joblog varchar(4000) = 'Job completed successfully'
				DECLARE @job_status varchar(100) ='SUCCESS'

				SELECT @jobstart=MIN(step_start_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_start_time>@last_jobrun_time
				SELECT @jobend=MAX(step_end_time) from meta_audit.transform_job_step_stats where job_id=@jobid and step_end_time>@last_jobrun_time
							
				IF EXISTS (SELECT * FROM meta_audit.transform_job_step_stats where step_status='FAIL' and job_id=@jobid)
				BEGIN
					SET @job_status='FAIL';
					SET @joblog='Failure occured at related SP';
				END

				--Insertion of third meta table
				PRINT 'Insertion of third meta table meta_audit.transform_job_stats'
				Insert meta_audit.transform_job_stats values (@jobid,@jobstart,@jobend,@job_status,@joblog,@pipelineid);	
			END
			ELSE
			BEGIN
				--Logging error due to incorrect jobid 
				PRINT 'into not rerun loop -error log'
				Insert meta_audit.transform_error_log_wsp SELECT ERROR_NUMBER() AS ErrorNumber,  
					ERROR_SEVERITY() AS ErrorSeverity,  
					ERROR_STATE() AS ErrorState,  
					ERROR_PROCEDURE() AS ErrorProcedure,  
					ERROR_MESSAGE() AS ErrorMessage,  
					@jobid AS ErrorJob,
					getdate() as ErrorDate; 
				PRINT 'error log - last insert'
				Insert meta_audit.transform_job_stats SELECT (select @jobid), null,null,(SELECT 'FAIL'),(SELECT 'Invalid job_id'),(SELECT @pipelineid);
			END
		END
	END TRY
	BEGIN CATCH
	--ERROR OCCURED
	PRINT 'ERROR IN WRAPPER SP SECTION'
	Insert meta_audit.transform_error_log_wsp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  ERROR_PROCEDURE() AS ErrorProcedure,  
		  ERROR_MESSAGE() AS ErrorMessage,  
		  @jobid AS ErrorJob,
		  getdate() as ErrorDate;
	PRINT 'LOGGING ERROR DETAILS OF JOB IN meta_audit.transform_job_stats'
	Insert meta_audit.transform_job_stats SELECT (select @jobid), getdate(),getdate(),(select 'FAIL'),ERROR_MESSAGE(),(select @pipelineid);
	IF EXISTS(SELECT a.name FROM sys.tables a WHERE a.name like 'er_log') 
	BEGIN
		DROP TABLE meta_ctl.er_log;
	END;
	create table meta_ctl.er_log
	with (
	DISTRIBUTION = ROUND_ROBIN
	)
	AS SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence, step_number, sp_name, sp_parms, dependent_step_num FROM meta_ctl.transform_job_steps where job_id=@jobid;

	DECLARE @er_counter int = 1
	DECLARE @er_recordCount int = (SELECT COUNT(*) from meta_ctl.er_log)
	
	WHILE @er_counter <= @er_recordCount 
	BEGIN
				DECLARE @erstep_number INT = (SELECT a.step_number FROM meta_ctl.er_log a where a.Sequence = @er_counter);
				Insert meta_audit.transform_job_step_stats select (select @jobid),(select @erstep_number),getdate(),getdate(),(select 'FAIL'),ERROR_MESSAGE(),NULL,NULL,(select @pipelineid);
				 SET @er_counter = @er_counter + 1
	END
	END CATCH
End
GO