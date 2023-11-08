SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- PROC to get the stage number lists based on batchid
CREATE PROC [meta_audit].[sp_transform_get_stage_list] @BatchID [NVARCHAR](300) AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQLString NVARCHAR(500)
    
    -- Check if the input BatchID is null
    IF LEN(TRIM(@BatchID)) > 0
	BEGIN
        --SET @SQLString = 'Select stage_number, COUNT(*) Total FROM meta_ctl.transform_job_control WHERE EnabledFlag=1 AND BatchID ='+@BatchID+'    GROUP BY stage_number';
        SET @SQLString = 'Select stage_number, COUNT(*) Total FROM meta_ctl.transform_job_control WHERE EnabledFlag=1 AND BatchID ='+@BatchID+' GROUP BY stage_number ORDER BY stage_number ASC';
    END
	--PRINT @SQLString
    EXEC sp_executesql @SQLString

END;
GO


