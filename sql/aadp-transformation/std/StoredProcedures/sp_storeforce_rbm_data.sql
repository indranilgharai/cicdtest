/****** Object:  StoredProcedure [std].[sp_storeforce_rbm_data]    Script Date: 2/3/2023 6:42:24 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_storeforce_rbm_data] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN IF EXISTS (
        SELECT
            TOP 1 *
        FROM
            stage.storeforce_rbm_data
    ) BEGIN TRUNCATE TABLE std.storeforce_rbm_data;

PRINT 'INSIDE LOOP'
INSERT INTO
    [std].[storeforce_rbm_data] (
        [Store_Code],
        [Store_Name],
        [Retail_Business_Code],
        [Retail_Business_Name],
        [Sub_Region_Code],
        [Sub_Region_Name],
        [Region_Code],
        [Region_Name],
        [Country],
        [md_record_written_timestamp],
        [md_record_written_pipeline_id],
        [md_transformation_job_id]
    )
SELECT
    RIGHT('000' + cast([Store Code] as [nvarchar](20)), 5) 'Store Code',
    cast([Store Name] as [nvarchar](250)) 'Store Name',
    cast([Retail Business Code] as [nvarchar](50)) 'Retail Business Code',
    cast([Retail Business Name] as [nvarchar](300)) 'Retail Business Name',
    cast([Sub Region Code] as [nvarchar](50)) 'Sub Region Code',
    cast([Sub Region Name] as [nvarchar](100)) 'Sub Region Name',
    cast([Region Code] as [nvarchar](50)) 'Region Code',
    cast([Region Name] as [nvarchar](50)) 'Region Name',
    cast([Country] as [nvarchar](10)) Country,
    getdate() AS [md_record_written_timestamp],
    @pipelineid [md_record_written_pipeline_id],
    @jobid [md_transformation_job_id]
FROM
    [stage].[storeforce_rbm_data] OPTION (LABEL = 'AADPSTDSFRBMD');

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPSTDSFRBMD' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label TRUNCATE TABLE stage.storeforce_rbm_data;

PRINT 'TRUNCATED STAGE'
END
ELSE BEGIN PRINT 'Stage is Empty'
END
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = max(md_record_written_timestamp)
FROM
    std.storeforce_rbm_data;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    std.storeforce_rbm_data
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
    'std.storeforce_rbm_data' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
