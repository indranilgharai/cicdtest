/****** Object:  StoredProcedure [std].[sp_hybris_shipped_orders]    Script Date: 23/03/2023 7:13:22 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_hybris_shipped_orders] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN

			/* Filter hybris_order_header for earliest shipped_date */
			with input_shipped_orders as (
				select
				hybris_order_id
				,shipped_date
				,created
				,md_record_ingestion_timestamp
				,md_record_ingestion_pipeline_id
				,md_source_system
				from [stage].[hybris_order_header]
				where status = 'SHIPPED'
			)
			,ranked_shipped_orders as (
				select *
				,rank() over(partition by hybris_order_id order by md_record_ingestion_timestamp asc, created asc, shipped_date asc) as order_rank
				from input_shipped_orders
			)
			,shipped_orders as (
				select distinct 
				concat('H',hybris_order_id) as order_id 
				,CASE WHEN shipped_date is NULL THEN NULL
							WHEN shipped_date='' THEN NULL
							WHEN upper(shipped_date) like '%Z' THEN convert(datetimeoffset,shipped_date)
							WHEN len(shipped_date) > 24 THEN convert(datetimeoffset,shipped_date)
							ELSE convert(datetimeoffset,concat(substring(shipped_date,1,19),substring(replace(shipped_date,':',''),18,3),
								':',substring(replace(shipped_date,':',''),21,2)) ) END shipped_date
				,CASE WHEN created is NULL THEN NULL
							WHEN created='' THEN NULL
							WHEN upper(created) like '%Z' THEN convert(datetimeoffset,created)
							WHEN len(created) > 24 THEN convert(datetimeoffset,created)
							ELSE convert(datetimeoffset,concat(substring(created,1,19),substring(replace(created,':',''),18,3),
								':',substring(replace(created,':',''),21,2)) ) END created
				,CAST(CONVERT(DATETIME, [md_record_ingestion_timestamp], 103) AS DATETIME) AS md_record_ingestion_timestamp
				,CAST([md_record_ingestion_pipeline_id] AS VARCHAR(200)) AS [md_record_ingestion_pipeline_id]
				,CAST([md_source_system] AS VARCHAR(100)) AS [md_source_system]
				,getdate() AS [md_record_written_timestamp]
				,@pipelineid AS [md_record_written_pipeline_id]
				,@jobid AS [md_transformation_job_id]
				from ranked_shipped_orders 
				where order_rank = 1
			)
			/* Merge into hybris_shipped_date if not matched
			If the record already exists in target do not update or insert */

			MERGE INTO std.hybris_shipped_orders AS TargetTbl
			USING shipped_orders as SourceTbl
			ON SourceTbl.order_id = TargetTbl.order_id

			WHEN NOT MATCHED BY TARGET THEN
			INSERT (order_id, shipped_date, created, md_record_ingestion_timestamp, md_record_ingestion_pipeline_id, md_source_system, md_record_written_timestamp, md_record_written_pipeline_id, md_transformation_job_id)
			VALUES (SourceTbl.order_id, SourceTbl.shipped_date, SourceTbl.created, SourceTbl.md_record_ingestion_timestamp, SourceTbl.md_record_ingestion_pipeline_id, SourceTbl.md_source_system,  SourceTbl.md_record_written_timestamp, SourceTbl.md_record_written_pipeline_id, SourceTbl.md_transformation_job_id);

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPSTDSHIPPEDORDERS'

			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.hybris_shipped_orders;

			SELECT @onlydate = CAST(@newrec AS DATE);
      
			DELETE FROM std.hybris_shipped_orders WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_hybris_shipped_orders' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO