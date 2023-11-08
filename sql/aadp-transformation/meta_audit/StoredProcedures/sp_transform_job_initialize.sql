SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- PROC identical with sp_ingest_job_initialize, it is used to for audit info. The records are stored at meta_audit.transform_load_status 
CREATE PROC [meta_audit].[sp_transform_job_initialize] @Target [VARCHAR](1000),@PipelineID [VARCHAR](100),@PipelineName [VARCHAR](100),@JobID [INT],@Source [VARCHAR](1000),@ZoneName [VARCHAR](30),@JobControlID [INT],@BatchID [INT],@BatchStartTime [DATETIME],@DataDate [DATETIME],@SourceStorageAccount [VARCHAR](50),@TargetStorageAccount [VARCHAR](50),@FileName [VARCHAR](max) AS
BEGIN
    DECLARE @ConvertedDataDate VARCHAR(30),
        @FilePositionSource INT,
        @FilePositionTarget INT,
        @FolderStructure VARCHAR(1000),
        @InsSource VARCHAR(1000),
        @InsTarget VARCHAR(1000),
        @SourceFormat VARCHAR(1000),
        @SourceType VARCHAR(1000),
        @SourceFormatSubType VARCHAR(1000),
        -- Changes done as part of Version 9.0
        @Frequency VARCHAR(1000),
        --Changes done for version 10.0
        @ProjectName VARCHAR(1000),
        @con_source VARCHAR(1000),
        @JobStartTime DATETIME,
        @Timezone VARCHAR(1000);

/*
    SET @Timezone = (
            SELECT Timezone
            FROM meta_ctl.ingest_batch_control
            WHERE BatchID = @BatchID
            )


    SET @ConvertedDataDate = FORMAT(@DataDate, 'yyyyMMddHHmm');
  
    SET @FilePositionSource = LEN(@Source) - (CHARINDEX('/', REVERSE(@Source)) - 1);
    SET @FilePositionTarget = LEN(@Target) - (CHARINDEX('/', REVERSE(@Target)) - 1);
    SET @SourceFormat = (
            SELECT DISTINCT SourceFormat
            FROM meta_ctl.ingest_job_control
            WHERE JobControlID = @JobControlID
            )
    SET @SourceType = (
            SELECT DISTINCT SourceType
            FROM meta_ctl.ingest_job_control
            WHERE JobControlID = @JobControlID
            )
    SET @SourceFormatSubType = (
            SELECT DISTINCT SourceFormatSubType
            FROM meta_ctl.ingest_job_control
            WHERE JobControlID = @JobControlID
            )
    -- Changes done as part of Version 9.0
    SET @Frequency = (
            SELECT DISTINCT UPPER(Frequency)
            FROM meta_ctl.ingest_job_control
            WHERE JobControlID = @JobControlID
            )
    --Changes done for version 10.0
    --Changes done for version 11.0 | Commented
    --SET @ProjectName= (SELECT SUBSTRING(TriggerName, 0, LEN(TriggerName)- LEN(RIGHT(TriggerName, LEN(TriggerName) - CHARINDEX ('_', TriggerName)))) from meta_ctl.ingest_trigger_control where BatchID=@BatchID) 
    --Changes done for version 11.0 | Added
*/

SET @ProjectName='AADP_TRANS';
SET @TimeZone='UTC';

/*
    SET @ProjectName = (
            SELECT ProjectName
            FROM meta_ctl.ingest_batch_control
            WHERE BatchID = @BatchID
            )
*/
    --SET @FolderStructure =
    --    (
    --	   SELECT 
    --			CASE
    --			-- Version 12.0--
    --			  --  WHEN @ZoneName = 'LND_RAW' and SourceFormatSubType <>'SAP BODS'
    --			   -- THEN CONCAT('y=', SUBSTRING(@ConvertedDataDate, 1, 4), '/m=', SUBSTRING(@ConvertedDataDate, 5, 2), '/d=', SUBSTRING(@ConvertedDataDate, 7, 2), '/hh=', SUBSTRING(@ConvertedDataDate, 9, 2), '/mm=', SUBSTRING(@ConvertedDataDate, 11, 2))
    ----			ELSE CONCAT('y=', SUBSTRING(@ConvertedDataDate, 1, 4), '/m=', SUBSTRING(@ConvertedDataDate, 5, 2), '/d=', SUBSTRING(@ConvertedDataDate, 7, 2))
    ----			Changes done as part of Version 9.0
    --				WHEN @Frequency = 'HOURLY'
    --				THEN CONCAT('y=', SUBSTRING(@ConvertedDataDate, 1, 4), '/m=', SUBSTRING(@ConvertedDataDate, 5, 2), '/d=', SUBSTRING(@ConvertedDataDate, 7, 2), '/h=', SUBSTRING(@ConvertedDataDate, 9, 2))
    --				ELSE CONCAT('y=', SUBSTRING(@ConvertedDataDate, 1, 4), '/m=', SUBSTRING(@ConvertedDataDate, 5, 2), '/d=', SUBSTRING(@ConvertedDataDate, 7, 2))
    --			END
    --	   FROM ctl_dev.ingest_job_control
    --	   where JobControlID = @JobControlID
    --    );
    --    SET @InsSource = (SELECT CASE WHEN @SourceFormatSubType='PSV_Bulk_Recursive' then @Source else CONCAT(SUBSTRING(@Source, 1, @FilePositionSource), @FolderStructure, '/', SUBSTRING(@Source, @FilePositionSource + 1, LEN(@Source)))END);
/*
    SET @InsSource = (
            SELECT CASE 
                    WHEN @SourceFormatSubType = 'PSV_Bulk_Recursive'
                        OR UPPER(@SourceFormatSubType) = 'RESTAPI'
                        THEN @Source
                    ELSE CONCAT (
                            SUBSTRING(@Source, 1, @FilePositionSource),
                            @FolderStructure,
                            '/',
                            SUBSTRING(@Source, @FilePositionSource + 1, LEN(@Source)),
                            '/',
                            @FileName
                            )
                    END
            );
    SET @con_source = (
            CASE 
                WHEN CHARINDEX('/', @Source) > 1
                    AND @ZoneName NOT IN ('STD_STD', 'SRC_RAW', 'LND_RAW')
                    THEN @InsSource
                ELSE @Source
                END
            )
*/
    SET @JobStartTime = (CAST(SYSDATETIMEOFFSET() AT TIME ZONE @Timezone AS DATETIME))

    -- version 12.0--
    --SET @InsTarget = (SELECT CASE WHEN @SourceFormatSubType='PSV_Bulk_Recursive' then @Target else CONCAT(SUBSTRING(@Target, 1, @FilePositionTarget), @FolderStructure, '/', SUBSTRING(@Target, @FilePositionTarget + 1, LEN(@Target)))END);
    --SET @Target = (select  TargetName from  ctl_dev.ingest_job_control where JobControlID= @JobControlID)
/*
    SELECT *
    FROM meta_audit.ingest_load_status
    WHERE JobControlID = @JobControlID;
*/

    BEGIN TRANSACTION;

    INSERT INTO meta_audit.transform_load_status (
        JobControlID,
        PipelineID,
        PipelineName,
        ZoneName,
        BatchID,
        JobID,
--        SourceStorageAccount,
--        Source,
--        TargetStorageAccount,
--        Target,
        JobStartTime,
        JobEndTime,
        BatchStartTime,
        BatchEndTime,
        DataDate,
        InputRowCount,
        TargetLoadCount,
        LoadStatus,
        LatestExecutionFlag,
        ErrorID,
        UserName,
        --Changes done for version 10.0
        ProjectName
        )
    VALUES (
        @JobControlID,
        @PipelineID,
        @PipelineName,
        @ZoneName,
        @BatchID,
        @JobID,
--        @SourceStorageAccount,
--        @con_source,
--        @TargetStorageAccount,
        --CASE
        --WHEN CHARINDEX('/', @Target) > 0
        --THEN @InsTarget
        --ELSE 
--        @Target,
        --END
        --CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AS DATETIME), 
        @JobStartTime,
        NULL,
        @JobStartTime,
        NULL,
        @DataDate,
        NULL,
        NULL,
        'Running',
        1,
        NULL,
        NULL,
        --Changes done for version 10.0
        @ProjectName
        );

    COMMIT TRANSACTION;
END;
GO


