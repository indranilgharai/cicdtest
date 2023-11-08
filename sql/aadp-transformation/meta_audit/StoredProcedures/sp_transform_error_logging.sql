SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [meta_audit].[sp_transform_error_logging] @PipelineID [VARCHAR](100),@JobID [INT],@ErrorCode [VARCHAR](50),@ErrorDescription [VARCHAR](8000),@ZoneName [VARCHAR](30),@ErrorType [VARCHAR](30),@JobControlID [INT],@BatchID [INT] AS
-- Error logging to handle all the processes at transformation pipeline
--Changes done for version 3.0
DECLARE @ProjectName VARCHAR(1000),
    @Timezone VARCHAR(1000),
    @ErrorLoggedTime DATETIME;

SET @ProjectName='AADP_TRANS';
SET @TimeZone='UTC';

/*
SET @Timezone = (
        SELECT Timezone
        FROM meta_ctl.ingest_batch_control
        WHERE BatchID = @BatchID
        )
SET @ProjectName = (
        SELECT SUBSTRING(TriggerName, 0, LEN(TriggerName) - LEN(RIGHT(TriggerName, LEN(TriggerName) - CHARINDEX('_', TriggerName))))
        FROM meta_ctl.ingest_trigger_control
        WHERE BatchID = @BatchID
        )
*/
SET @ErrorLoggedTime = (CAST(SYSDATETIMEOFFSET() AT TIME ZONE @Timezone AS DATETIME))

INSERT INTO meta_audit.transform_error_logs (
    PipelineID,
    JobControlID,
    ZoneName,
    BatchID,
    JobID,
    ErrorType,
    ErrorCode,
    ErrorDescription,
    ErrorLoggedTime,
    --Changes done for version 3.0
    ProjectName
    )
VALUES (
    @PipelineID,
    @JobControlID,
    @ZoneName,
    @BatchID,
    @JobID,
    @ErrorType,
    @ErrorCode,
    @ErrorDescription,
    --GETDATE()
    --CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AS DATETIME),
    @ErrorLoggedTime,
    --Changes done for version 3.0
    @ProjectName
    );

UPDATE meta_audit.transform_load_status
SET LoadStatus = 'Failed'
,JobEndTime=@ErrorLoggedTime
WHERE JobControlID = @JobControlID
    AND PipelineID = @PipelineID
	AND ZoneName = @ZoneName	

UPDATE meta_audit.transform_load_status
SET ErrorID = el.ErrorID
FROM meta_audit.transform_load_status ls
INNER JOIN meta_audit.transform_error_logs el
    ON ls.PipelineID = el.PipelineID
        AND ls.JobID = el.Jobid
WHERE ls.PipelineID = @PipelineID
    AND ls.JobControlID = @JobControlID
	AND ls.ZoneName = @ZoneName
GO


