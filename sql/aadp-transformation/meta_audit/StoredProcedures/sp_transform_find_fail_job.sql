SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [meta_audit].[sp_transform_find_fail_job] @ProcName [NVARCHAR](100),@RunTime [NVARCHAR](20),@ErrorNumber [INT] OUT,@ErrorSeverity [INT] OUT,@ErrorState [INT] OUT,@ErrorMessage [NVARCHAR](1000) OUT AS
-- Proc to find failed job at table meta_audit.transform_error_log_sp 
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQLString NVARCHAR(500),
    @ParamDefinition NVARCHAR(500) 

	SET @SQLString = N'select top 1 @ErrorNumber=ErrorNumber, @ErrorSeverity=ErrorSeverity, @ErrorState=ErrorState, @ErrorMessage=ErrorMessage from meta_audit.transform_error_log_sp
where ErrorProcedure ='''+@ProcName+''' and updated_date >= '''+@RunTime+''' order by updated_date DESC ';

	SET @ParamDefinition = N'@ErrorNumber INT OUTPUT, @ErrorSeverity INT OUTPUT, @ErrorState INT OUTPUT, @ErrorMessage NVARCHAR(1000) OUTPUT';
	EXEC sp_executesql @SQLString, @ParamDefinition, @ErrorNumber=@ErrorNumber OUTPUT, @ErrorSeverity=@ErrorSeverity OUTPUT, @ErrorState=@ErrorState OUTPUT, @ErrorMessage=@ErrorMessage OUTPUT;

END;
GO


