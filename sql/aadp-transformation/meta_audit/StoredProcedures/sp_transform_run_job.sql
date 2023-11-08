SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-- This is a main Proc to handle running each job
CREATE PROC [meta_audit].[sp_transform_run_job] @ProcName [nvarchar](200),@JobID [INT],@stage_number [INT],@resetFlag [BIT],@PipelineID [varchar](200) AS
BEGIN
DECLARE
@Stmt NVARCHAR(200),
@ErrorNumber INT,
@ErrorSeverity INT,
@ErrorState INT,
@ErrorMessage NVARCHAR(1000),
@JobStartDate NVARCHAR(30),
@ProcessStatus NVARCHAR(20),
@ParamDefinition NVARCHAR(500),
@DependentJobs NVARCHAR(100),
@TotalFailParentJob INT,
@ProcessJob BIT;

SET @ProcessStatus='Completed';
SET @Stmt= N'EXEC '+ @ProcName +' @JobID, @stage_number, @resetFlag, @PipelineID';
SET @ParamDefinition = N'@JobID INT, @stage_number INT, @resetFlag BIT, @PipelineID nvarchar(200)';
SET @ProcessJob=1;
SET @JobStartDate=getdate();
	BEGIN TRY
        -- get the list of dependent jobs for a selected JobID
        EXEC meta_audit.sp_transform_find_parent_job @JobID, @Result=@DependentJobs OUTPUT;
        PRINT @DependentJobs;

        IF LEN(@DependentJobs) >=1
        BEGIN  
        -- Check all the process parent jobs status
            EXEC meta_audit.sp_transform_get_fail_parent_job @DependentJobs, @PipelineID, @Result=@TotalFailParentJob OUTPUT;
            PRINT @TotalFailParentJob;
            IF (@TotalFailParentJob >0)
            BEGIN
                SET @ProcessJob = 0;
            END
        END
        
        IF (@ProcessJob = 1)
        BEGIN
        -- executing the job
            EXECUTE sp_executesql @Stmt, @ParamDefinition, @JobID= @JobID, @stage_number = @stage_number, @resetFlag= @resetFlag, @PipelineID= @PipelineID;
        -- get any errors from meta_audit.transform_error_log_sp
			EXEC meta_audit.sp_transform_find_fail_job @ProcName, @JobStartDate, @ErrorNumber=@ErrorNumber OUTPUT, @ErrorSeverity=@ErrorSeverity OUTPUT, @ErrorState=@ErrorState OUTPUT, @ErrorMessage=@ErrorMessage  OUTPUT;
			IF @ErrorMessage IS NOT NULL
            SET @ProcessStatus = 'Fail'; 
        END
        ELSE
        BEGIN
            SET @ErrorNumber = 50001;
            SET @ErrorSeverity = 10;
            SET @ErrorState = 1;
            SET @ErrorMessage = 'The dependent job(s) Fail';
            SET @ProcessStatus = 'Fail';
        END
	END TRY
	BEGIN CATCH
		SET @ErrorNumber = ERROR_NUMBER();
		SET @ErrorSeverity = ERROR_SEVERITY();
		SET @ErrorState = ERROR_STATE();
		SET @ErrorMessage = ERROR_MESSAGE();
		SET @ProcessStatus = 'Fail';
	END CATCH

SELECT 
@ProcessStatus AS ProcessStatus
,@ErrorNumber AS ErrorNumber
,@ErrorSeverity AS ErrorSeverity
,@ErrorState AS ErrorState
,@ProcName AS ErrorProcedure
,@ErrorMessage AS ErrorMessage
,@JobStartDate AS Updated_date

END;
GO


