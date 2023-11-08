SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [meta_audit].[sp_transform_find_parent_job] @JobID [INT],@Result [NVARCHAR](100) OUT AS
BEGIN
-- PROC to find dependent job(s) related to a particular JobID
-- It is assumed the JobID is unique
    SET NOCOUNT ON;
    --SET @Result=0;
    DECLARE @SQLString NVARCHAR(500),
    @ParamDefinition NVARCHAR(500) 
    SET @Result=0;
	SET @SQLString = N'SELECT @Result=dependent_job
    FROM [meta_ctl].[transform_job_control]
    WHERE JobID='+CAST(@JobID AS VARCHAR(10));

	SET @ParamDefinition = N'@Result NVARCHAR(100) OUTPUT';
	EXEC sp_executesql @SQLString, @ParamDefinition, @Result=@Result OUTPUT;

END;
GO


