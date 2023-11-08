SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- PROC to find any failed dependent job on the same PipelineID
CREATE PROC [meta_audit].[sp_transform_get_fail_parent_job] @ParentJobID [NVARCHAR](100),@PipelineID [NVARCHAR](100),@Result [TINYINT] OUT AS
BEGIN
--    SET NOCOUNT ON;
    DECLARE @SQLString NVARCHAR(500),
    @ParamDefinition NVARCHAR(500) 
    SET @Result=0;
	SET @SQLString = 'SELECT @Result=count(*)
    FROM [meta_audit].[transform_error_logs]
    WHERE JobID IN ('+@ParentJobID+') AND PipelineID ='''+@PipelineID+''''; 
	SET @ParamDefinition = N'@Result TINYINT OUTPUT';
	EXEC sp_executesql @SQLString, @ParamDefinition, @Result=@Result OUTPUT;

END;
GO


