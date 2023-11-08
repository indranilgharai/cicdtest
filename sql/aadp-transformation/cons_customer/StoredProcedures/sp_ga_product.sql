/****** Object:  StoredProcedure [cons_customer].[sp_ga_product]    Script Date: 5/19/2022 10:36:21 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE  PROC [cons_customer].[sp_ga_product] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
	/* Reset value is passed when the Job is triggered
	*/
		IF @reset = 0
		BEGIN

/*DELETE FROM TARGET IF DATA FOR THE DATES AVAILABLE IN STG IS ALREADY PRESENT, 
AND RELOAD DATES [AVAILABLE IN STG] FROM STD TO CONS*/

delete from cons_customer.ga_product
where "date" in (select distinct "date" from [stage].[bq_ga_sessions_raw])

insert into cons_customer.ga_product
select distinct  
concat(visitstarttime,fullvisitorid,visitid) concat_key,
cast([date] as date) [date],
[visitStartTime],
[fullVisitorId],
[VisitID],
[userId],
[transactionId],
[action_type],
[step],
[v2ProductCategory],
[productRevenue],
[productQuantity],
[cd_4],
[md_record_ingestion_timestamp],
getdate() md_record_written_timestamp
from cons_customer.ga_product_wide WITH (NOLOCK) 
where "date" in (select distinct "date" from [stage].[bq_ga_sessions_raw])
and productSKU is not null 
	OPTION (LABEL = 'AADCONSGARWSS');
					   
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label = 'AADCONSGARWSS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label


		END
		ELSE
			--##uncomment it after logic for loading column : md_record_written_timestamp is finalised##
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_customer.ga_product;
			SELECT @onlydate = CAST(@newrec AS DATE);
			--PRINT @onlydate
			DELETE FROM cons_customer.ga_product WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.sp_ga_product' AS ErrorProcedure
			,-- here the sp name u give should be exact same value u give for this sp in sp_name column of meta table: [meta_ctl].[transformation_job_steps]
			ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END