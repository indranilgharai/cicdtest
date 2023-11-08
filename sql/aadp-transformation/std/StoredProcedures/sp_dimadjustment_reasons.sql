/****** Object:  StoredProcedure [std].[sp_DimAdjustment_Reasons]    Script Date: 12/14/2022 10:40:04 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/***Stored procedure to insert reason code data to target table***/
CREATE PROC [std].[sp_dimadjustment_reasons] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY 
    IF @reset = 0 
    BEGIN 
    DECLARE @max_ingestion_date_cons [varchar](500)
    SELECT
        @max_ingestion_date_cons = MAX(CAST([md_record_written_timestamp] AS date))
    FROM
        [std].[dimadjustment_reasons];
		
	DELETE FROM [std].[dimadjustment_reasons] 
	WHERE concat(adjustment_code,adjustment_group) in (SELECT concat([Reason_Code],[Adjustment_Group])
														FROM [stage].[dimadjustment_reasons]);

	INSERT INTO [std].[dimadjustment_reasons] ( 
		adjustment_code,
		adjustment_reason,
		adjustment_group,
		controllable_flag,
		md_record_written_timestamp,
		md_record_written_pipeline_id,
		md_transformation_job_id)
	
	SELECT [Reason_Code] AS adjustment_code,
		[Reason_Code_Description] AS adjustment_reason,
		[Adjustment_Group] AS adjustment_group,
		CASE WHEN [Reason_Code] in ('R01','R03','R12') THEN 'Y' ELSE 'N' END AS controllable_flag,
		getDate() AS md_record_written_timestamp,
		@pipelineid AS md_record_written_pipeline_id,
		@jobid AS md_transformation_job_id
	FROM [stage].[dimadjustment_reasons];
	
--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPDIMADJRES' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
    
    TRUNCATE TABLE [stage].[dimadjustment_reasons] 
    
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = MAX(md_record_written_timestamp)
FROM
    std.dimadjustment_reasons;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    std.dimadjustment_reasons
WHERE
    md_record_written_timestamp = @newrec;

END
END TRY BEGIN CATCH --ERROR OCCURED
PRINT 'ERROR SECTION INSERT'
INSERT
    meta_audit.transform_error_log_sp
SELECT
    ERROR_NUMBER() AS ErrorNumber,
    ERROR_SEVERITY() AS ErrorSeverity,
    ERROR_STATE() AS ErrorState,
    'std.sp_dimadjustment_reasons' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
