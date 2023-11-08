SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_store_sku_daily_sales_rate] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			DECLARE @read_count [int]
			select @read_count=count(*) from cons_customer.sales_detail_time;
			if (@read_count)>0
			BEGIN
			
			TRUNCATE TABLE [std].[netsuite_store_sku_daily_sales_rate];

			INSERT INTO [std].[netsuite_store_sku_daily_sales_rate]
			SELECT  
			[store_location_code],
			[item_code],
			sales_units,
			cast(sales_units/28.0 as float)  as daily_sales_rate ,
			getdate() as md_record_written_timestamp,
			@pipelineid   AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id,
			'DERIVED' as md_source_system
			from (
				select distinct 
				origin_store as [store_location_code]
				,SKU_number AS [item_code]
				,sum(sales_units) as sales_units
				FROM cons_customer.sales_detail_time			
				where cast(dateadd(day,-28,getdate()) as date)<=receipt_date and receipt_date <=getdate() 
				group by origin_store,SKU_number
			)a
			OPTION (LABEL = 'AADSTDDSLSRT');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDDSLSRT'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			END				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[netsuite_store_sku_daily_sales_rate] ;
			
			delete from std.[netsuite_store_sku_daily_sales_rate] where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_netsuite_store_sku_daily_sales_rate' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date

END CATCH

END