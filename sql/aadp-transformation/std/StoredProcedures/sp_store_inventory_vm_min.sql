/****** Object:  StoredProcedure [std].[sp_store_inventory_vm_min]    Script Date: 1/23/2023 10:23:08 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_store_inventory_vm_min] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN BEGIN TRY IF @reset = 0 BEGIN IF EXISTS (
		SELECT
			TOP 1 *
		FROM
			stage.dwh_store_inventory_vm_min
	) BEGIN 
	
	TRUNCATE TABLE std.store_inventory_vm_min;

PRINT 'INSIDE LOOP'
insert into [std].[store_inventory_vm_min] 
SELECT 
	  concat([sbs_no],[store_no],[description1]) as storeinvkey
	  ,[sbs_no]
      ,[store_no]
      ,cast([description1] as varchar(100)) as [description1]
      ,[vm_min]
      ,[vm_min_old]
      ,[sellable_stock]
      ,[sellable_stock_old]
      ,convert(datetime,[md_record_ingestion_timestamp] ,105) as [md_record_ingestion_timestamp]
      ,[md_record_ingestion_pipeline_id]
      ,[md_source_system]
	, getdate() as [md_record_written_timestamp] 
	,@pipelineid [md_record_written_pipeline_id] 
	,@jobid [md_transformation_job_id] 
from [stage].[dwh_store_inventory_vm_min]
OPTION (LABEL = 'AADPSTDDWHSTRINV');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPSTDDWHSTRINV'

			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label


	
TRUNCATE TABLE [stage].[dwh_store_inventory_vm_min];

PRINT 'TRUNCATED STAGE'
END
ELSE BEGIN PRINT 'Stage is Empty'
END
END
ELSE BEGIN 



			DECLARE @newrec DATETIME,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp) FROM std.[dwh_store_inventory_vm_min];

			SELECT @onlydate = CAST(@newrec AS DATE);

			DELETE FROM std.[dwh_store_inventory_vm_min] WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_store_inventory_vm_min' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
