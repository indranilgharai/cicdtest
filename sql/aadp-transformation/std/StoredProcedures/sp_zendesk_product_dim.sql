/****** Object:  StoredProcedure [cons_customer].[sp_zendesk_contact_reason]   Script Date: 15/03/2023 04:05:00 PM ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_product_dim] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN


    IF OBJECT_ID('tempdb..#proddim') IS NOT  NULL
    BEGIN
        DROP TABLE #proddim
    END
    create table #proddim
    with
    (distribution=round_robin,
    clustered index(ticket_id)
    )
    as 

    Select ticket_id,
    f.value as product,
	CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
	[md_record_ingestion_pipeline_id],
	[md_source_system],
    getdate() as [md_record_written_timestamp]
	,@pipelineid [md_record_written_pipeline_id]
    ,@jobid [md_transformation_job_id]

    from std.zendesk_custom_fields as s
    CROSS APPLY STRING_SPLIT(s.[prod], '"') as f
    where value not in ('[',']',',')

    MERGE
    INTO std.zendesk_product_dim as TargetTbl
    USING #proddim as SourceTbl
    ON  concat(SourceTbl.ticket_id,SourceTbl.[product] )= concat(TargetTbl.ticket_id, TargetTbl.[product])
    -- WHEN MATCHED 
    -- THEN UPDATE SET
    -- TargetTbl.[ticket_id]	=SourceTbl.[ticket_id]	,
    -- TargetTbl.[product]	=SourceTbl.[product],
	-- TargetTbl.[md_record_ingestion_timestamp]	=SourceTbl.[md_record_ingestion_timestamp],
	-- TargetTbl.[md_record_ingestion_pipeline_id]	=SourceTbl.[md_record_ingestion_pipeline_id],
	-- TargetTbl.[md_source_system]	=SourceTbl.[md_source_system],
	-- TargetTbl.[md_record_written_timestamp]	=SourceTbl.[md_record_written_timestamp],
	-- TargetTbl.[md_record_written_pipeline_id]	=SourceTbl.[md_record_written_pipeline_id],
	-- TargetTbl.[md_transformation_job_id]	=SourceTbl.[md_transformation_job_id]	
    WHEN NOT MATCHED BY TARGET
    THEN 
    INSERT 
    ([ticket_id], 
	[product],
	[md_record_ingestion_timestamp],
	[md_record_ingestion_pipeline_id],
	[md_source_system],
    [md_record_written_timestamp],
	[md_record_written_pipeline_id],
    [md_transformation_job_id]
	)
    VALUES 
    (SourceTbl.[ticket_id], 
	SourceTbl.[product],
	SourceTbl.[md_record_ingestion_timestamp],
	SourceTbl.[md_record_ingestion_pipeline_id],
	SourceTbl.[md_source_system],
    SourceTbl.[md_record_written_timestamp],
	SourceTbl.[md_record_written_pipeline_id],
    SourceTbl.[md_transformation_job_id]
	)	;




	UPDATE STATISTICS [std].[zendesk_product_dim];
	
		--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPCONSSALES'

			EXEC meta_ctl.sp_row_count @jobid ,@step_number ,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM [std].[zendesk_product_dim];
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM [std].[zendesk_product_dim] WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.zendesk_product_dim' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END