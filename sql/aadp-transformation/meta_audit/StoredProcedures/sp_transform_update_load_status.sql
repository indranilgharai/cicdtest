SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- This proc is the same as sp_ingest_update_load_status except it is used for transformation pipeline with different table destination
CREATE PROC [meta_audit].[sp_transform_update_load_status] @PipelineID [VARCHAR](100),@JobID [INT],@InputRowCount [INT],@TargetLoadCount [INT],@MetadataFileInputRowCount [INT],@MetadataFileTargetRowCount [INT],@ZoneName [VARCHAR](30),@JobControlID [INT],@BatchID [INT],@FileTimeStamp [VARCHAR](1000),@FileName [VARCHAR](1000),@MetaDataFileTotalName [VARCHAR](1000),@MainFileArchived [VARCHAR](1000),@LoadStatus [VARCHAR](1000) AS
BEGIN
    DECLARE @Timezone VARCHAR(1000);
	SET @Timezone ='UTC'
	/*
    SET @Timezone = (
            SELECT Timezone
            FROM meta_ctl.ingest_batch_control
            WHERE BatchID = @BatchID
            )
*/
    UPDATE meta_audit.transform_load_status
    SET
        JobEndTime = CAST(SYSDATETIMEOFFSET() AT TIME ZONE @Timezone AS DATETIME),
        InputRowCount = CASE 
            WHEN InputRowCount IS NULL
                THEN @InputRowCount
            ELSE InputRowCount
            END,
        TargetLoadCount = CASE 
            WHEN TargetLoadCount IS NULL
                THEN @TargetLoadCount
            ELSE TargetLoadCount
            END,
        FileTimeStamp = convert(NUMERIC, @FileTimeStamp),
        --ingest_load_status = case when ingest_load_status is null then @ingest_load_status
        --      else ingest_load_status end,
        LoadStatus = @LoadStatus,
        metadatafiletotalname = CASE 
            WHEN metadatafiletotalname IS NULL
                THEN @MetaDataFileTotalName
            ELSE metadatafiletotalname
            END,
        MetadataFileInputRowCount = CASE 
            WHEN MetadataFileInputRowCount IS NULL
                THEN @MetadataFileInputRowCount
            ELSE MetadataFileInputRowCount
            END,
        MetadataFileTargetRowCount = CASE 
            WHEN MetadataFileTargetRowCount IS NULL
                THEN @MetadataFileTargetRowCount
            ELSE MetadataFileTargetRowCount
            END,
        MainFileArchived = CASE 
            WHEN MainFileArchived IS NULL
                THEN @MainFileArchived
            ELSE MainFileArchived
            END
    WHERE JobControlID = @JobControlID
        AND PipelineID = @PipelineID
        AND (ZoneName=@ZoneName or  [source] LIKE ('%' + @FileName + '%'))
        AND LatestExecutionFlag = 1
END;
GO


