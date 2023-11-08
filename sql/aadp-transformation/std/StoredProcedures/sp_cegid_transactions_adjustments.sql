/****** Object:  StoredProcedure [std].[sp_cegid_transactions_adjustments]    Script Date: 12/8/2022 4:04:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_cegid_transactions_adjustments] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			
			INSERT INTO [std].[cegid_transactions_adjustments]
			select distinct 
			document_date,
			document_type,
			document_store,
			document_warehouse,
			document_number,
			document_internal_reference,
			line_number,
			item_code,
			cast(replace(replace(quantity,char(10),''),char(13),'') as float) as quantity,
			case when document_type = 'INV' 
				then 'INV'
				else reason_code
				end as reason_code,
			getdate() as md_record_written_timestamp,
			@pipelineid AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'CEGID' as md_source_system
			from [stage].[cegid_transactions_adjustments] 
			where item_code is not null;

			IF OBJECT_ID('tempdb..#cegid_transactions_adjustments_temp') IS NOT NULL
			BEGIN
				DROP TABLE #cegid_transactions_adjustments_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #cegid_transactions_adjustments_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select  
				[document_date],
				[document_type],
				[document_store],
				[document_warehouse],
				[document_number],
				[document_internal_reference]L,
				[line_number],
				[item_code],
				[quantity],
				[reason_code] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (
				SELECT *, rank() OVER (PARTITION BY document_internal_reference,line_number,document_number ORDER BY md_record_written_timestamp desc) AS dupcnt
				FROM [std].[cegid_transactions_adjustments] )a WHERE dupcnt=1 ;

			truncate table [std].[cegid_transactions_adjustments];
			
			insert into [std].[cegid_transactions_adjustments]
			select * from #cegid_transactions_adjustments_temp
			OPTION (LABEL = 'AADSTDTRNADJ');

			DROP TABLE #cegid_transactions_adjustments_temp

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDTRNADJ'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label	
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[cegid_transactions_adjustments] ;
			
			delete from [std].[cegid_transactions_adjustments] where md_record_written_timestamp=@newrec;
			
		END
	END TRY
	
	BEGIN CATCH
	
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_cegid_transactions_adjustments' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	
	
	END CATCH

END
