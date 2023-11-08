SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- PROC to get the jobs based on batchid and stage number
CREATE PROC [meta_audit].[sp_transform_get_job_list] @BatchID [NVARCHAR](300),@stage_number [NVARCHAR](3) AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQLString NVARCHAR(500)
    
    -- Check if the input BatchID is null
    IF LEN(TRIM(@BatchID)) > 0
	BEGIN
--        SET @SQLString = 'Select JobID, SourceFormatSubType sp_name FROM meta_ctl.transform_job_control WHERE EnabledFlag=1 AND BatchID ='+@BatchID+' AND stage_number='+ @stage_number;
        SET @SQLString = 'Select * FROM meta_ctl.transform_job_control WHERE EnabledFlag=1 AND BatchID ='+@BatchID+' AND stage_number='+ @stage_number;
    END
	--PRINT @SQLString
    EXEC sp_executesql @SQLString

END;
GO


