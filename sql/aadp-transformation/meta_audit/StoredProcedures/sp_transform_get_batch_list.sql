SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- PROC to get the list of batchs
CREATE PROC [meta_audit].[sp_transform_get_batch_list] @BatchID [NVARCHAR](300) AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQLString NVARCHAR(500)
    
    -- Check if the input BatchID is null
    IF ((TRIM(@BatchID) ='' OR @BatchID IS NULL))
	BEGIN
        SET @SQLString = 'Select BatchID, COUNT(*) Total FROM meta_ctl.transform_job_control WHERE EnabledFlag=1  GROUP BY BatchID';
    END
    ELSE
    BEGIN
        SET @SQLString = 'Select BatchID, COUNT(*) Total FROM meta_ctl.transform_job_control ' + ' WHERE EnabledFlag=1 AND BatchID IN ('+@BatchID+') '+'   GROUP BY BatchID';
    END
	--PRINT @SQLString
    EXEC sp_executesql @SQLString

END;
GO


