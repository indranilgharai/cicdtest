/****** Object:  StoredProcedure [std].[sp_itemcost_excluded_stock_list]    Script Date: 2/2/2023 1:42:27 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_itemcost_excluded_stock_list] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 

BEGIN BEGIN TRY IF @reset = 0 BEGIN IF EXISTS (
		SELECT
			TOP 1 *
		FROM
			stage.itemcost_excluded_stock_list
	) BEGIN 
	
	TRUNCATE TABLE std.itemcost_excluded_stock_list;


				
				insert into [std].[itemcost_excluded_stock_list]
				select FORMAT(CAST(Store_no AS INT),'00000','en-US') Store_no,Store,SKU,Description,
				getdate() as md_record_written_timestamp,
    @pipelineid as md_record_written_pipeline_id,
    @jobid as md_transformation_job_id
				from stage.itemcost_excluded_stock_list
				

				
				UPDATE STATISTICS [std].[itemcost_excluded_stock_list];
        

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADITEMCOSTEXCL'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label	

				
TRUNCATE TABLE stage.itemcost_excluded_stock_list;

PRINT 'TRUNCATED STAGE'
END
ELSE BEGIN PRINT 'Stage is Empty'
END
END
ELSE BEGIN 

		DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[itemcost_excluded_stock_list] ;
			
			delete from [std].[itemcost_excluded_stock_list] where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_itemcost_excluded_stock_list' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END
