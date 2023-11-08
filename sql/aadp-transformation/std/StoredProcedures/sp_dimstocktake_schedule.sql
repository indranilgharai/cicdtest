/****** Object:  StoredProcedure [std].[sp_dimstocktake_schedule]    Script Date: 12/14/2022 10:38:49 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_dimstocktake_schedule] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 

BEGIN BEGIN TRY IF @reset = 0 BEGIN IF EXISTS (
		SELECT
			TOP 1 *
		FROM
			stage.dimstocktake_schedule
	) BEGIN 
	
	TRUNCATE TABLE std.dimstocktake_schedule;

PRINT 'INSIDE LOOP'


    DECLARE @max_ingestion_date_cons [varchar](500)
    SELECT
        @max_ingestion_date_cons = MAX(CAST([md_record_written_timestamp] AS date))
    FROM
        [std].[dimstocktake_schedule];

	INSERT INTO [std].[dimstocktake_schedule]([locationkey]
		,[source_system]
		,[stocktake_name]
		,[stocktake_qtr]
		,[stocktake_year]
		,[stocktake_date]
		,[last_stocktake_name]
		,[last_stocktake_qtr]
		,[last_stocktake_year]
		,[last_stocktake_date]
		,[md_record_written_timestamp]
		,[md_record_written_pipeline_id]
		,[md_transformation_job_id]
		)
	
	SELECT CAST([locationkey] AS varchar(10))
		,CAST([source_system] AS varchar(20))
		,CAST([stocktake_name] AS varchar(50))
		,CAST([stocktake_qtr] AS varchar(5))
		,CAST([stocktake_year] AS int)
		,CAST([stocktake_date] as date)
		,CAST([last_stocktake_name] AS varchar(50))
		,CAST([last_stocktake_qtr] AS varchar(5))
		,CAST([last_stocktake_year] AS int)
		,CAST([last_stocktake_date] as date)
		,getdate() AS md_record_written_timestamp
		,@pipelineid AS md_record_written_pipeline_id
		,@jobid AS md_transformation_job_id
	FROM [stage].[dimstocktake_schedule];
	
	
	
--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPDIMSTKSCH' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label

	
TRUNCATE TABLE [stage].[dimstocktake_schedule];

PRINT 'TRUNCATED STAGE'
END
ELSE BEGIN PRINT 'Stage is Empty'
END
END
ELSE BEGIN 

DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = MAX(md_record_written_timestamp)
FROM
    [std].[dimstocktake_schedule];

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    [std].[dimstocktake_schedule]
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
    'std.sp_dimstocktake_schedule' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
