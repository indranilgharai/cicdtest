/****** Object:  StoredProcedure [cons_retail].[sp_store_kpi_monthly]    Script Date: 12/20/2022 1:51:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_store_kpi_monthly] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN 



DECLARE @batch_date [varchar](500)
select @batch_date=
	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(month,-4,getdate())
	else dateadd(month,-4,max(CAST(date_key as date))) end
from
	cons_retail.store_kpi_monthly;

IF OBJECT_ID('tempdb..#storekpimonthly') IS NOT  NULL
BEGIN
    DROP TABLE #storekpimonthly
END
create table #storekpimonthly
with
(distribution=round_robin,
clustered index(storekpikey)
)
as 


/*****aggregating store_kpi data at month level*****/
SELECT
  CAST(
    concat(
      format(DATEADD(DAY, 1, EOMONTH(date_key, -1)), 'yyyyMMdd'),
      location_key
    ) AS varchar(250)
  ) AS storekpikey,
  [location_key] as [location_key],
  DATEADD(DAY, 1, EOMONTH(date_key, -1)) as [date_key],
  sum([revenue_in_aud]) as [revenue_in_aud],
  sum([revenue_in_aud_LY]) as [revenue_in_aud_LY],
  sum([target_aud]) as [target_aud],
  sum([target_aud_LY]) as [target_aud_LY],
  sum([budget_aud]) as [budget_aud],
  sum([budget_aud_LY]) as [budget_aud_LY],
  sum([transactions]) as [transactions],
  sum([transactions_LY]) as [transactions_LY],
  sum([multi_unit_transactions]) as [multi_unit_transactions],
  sum([multi_unit_transactions_LY]) as [multi_unit_transactions_LY],
  sum([new_customer_transactions]) as [new_customer_transactions],
  sum([new_customer_transactions_LY]) as [new_customer_transactions_LY],
  sum([linked_transactions]) as [linked_transactions],
  sum([linked_transactions_LY]) as [linked_transactions_LY],
  sum([multi_category_transactions]) as [multi_category_transactions],
  sum([multi_category_transactions_LY]) as [multi_category_transactions_LY],
  sum([units]) as [units],
  sum([units_LY]) as [units_LY],
  sum([traffic]) as [traffic],
  sum([traffic_LY]) as [traffic_LY],
  sum([shopfront_traffic_open]) as [shopfront_traffic_open],
  sum([shopfront_traffic_open_LY]) as [shopfront_traffic_open_LY],
  sum([shopfront_traffic_closed]) as [shopfront_traffic_closed],
  sum([shopfront_traffic_closed_LY]) as [shopfront_traffic_closed_LY],
  sum([bounces60]) as [bounces60],
  sum([bounces60_LY]) as [bounces60_LY],
  sum([bounces120]) as [bounces120],
  sum([bounces120_LY]) as [bounces120_LY],
  sum([in_store_secs]) as [in_store_secs],
  sum([in_store_secs_LY]) as [in_store_secs_LY],
  sum([skincare_revenue_aud]) as [skincare_revenue_aud],
  sum([skincare_revenue_aud_LY]) as [skincare_revenue_aud_LY],
  sum([bodycare_revenue_aud]) as [bodycare_revenue_aud],
  sum([bodycare_revenue_aud_LY]) as [bodycare_revenue_aud_LY],
  sum([fragrance_revenue_aud]) as [fragrance_revenue_aud],
  sum([fragrance_revenue_aud_LY]) as [fragrance_revenue_aud_LY],
  sum([haircare_revenue_aud]) as [haircare_revenue_aud],
  sum([haircare_revenue_aud_LY]) as [haircare_revenue_aud_LY],
  sum([home_revenue_aud]) as [home_revenue_aud],
  sum([home_revenue_aud_LY]) as [home_revenue_aud_LY],
  sum([kits_revenue_aud]) as [kits_revenue_aud],
  sum([kits_revenue_aud_LY]) as [kits_revenue_aud_LY],
  getdate() as md_record_written_timestamp,
  @pipelineid as md_record_written_pipeline_id,
  @jobid as md_transformation_job_id
FROM
  [cons_retail].[store_kpi_daily]
  where date_key>=  DATEADD(DAY, 1, EOMONTH(@batch_date, -1)) 
group by
  location_key,
  DATEADD(DAY, 1, EOMONTH(date_key, -1)) 
  OPTION (LABEL = 'AADPRETKPISTOKPIMNTHLY');




MERGE
INTO cons_retail.store_kpi_monthly as TargetTbl
USING #storekpimonthly as SourceTbl
ON  SourceTbl.storekpikey= TargetTbl.storekpikey
WHEN MATCHED 
THEN UPDATE SET
TargetTbl.[storekpikey]	=SourceTbl.[storekpikey]	,
TargetTbl.[location_key]	=SourceTbl.[location_key]	,
TargetTbl.[date_key]	=SourceTbl.[date_key]	,
TargetTbl.[revenue_in_aud]	=SourceTbl.[revenue_in_aud]	,
TargetTbl.[revenue_in_aud_LY]	=SourceTbl.[revenue_in_aud_LY]	,
TargetTbl.[target_aud]	=SourceTbl.[target_aud]	,
TargetTbl.[target_aud_LY]	=SourceTbl.[target_aud_LY]	,
TargetTbl.[budget_aud]	=SourceTbl.[budget_aud]	,
TargetTbl.[budget_aud_LY]	=SourceTbl.[budget_aud_LY]	,
TargetTbl.[transactions]	=SourceTbl.[transactions]	,
TargetTbl.[transactions_LY]	=SourceTbl.[transactions_LY]	,
TargetTbl.[multi_unit_transactions]	=SourceTbl.[multi_unit_transactions]	,
TargetTbl.[multi_unit_transactions_LY]	=SourceTbl.[multi_unit_transactions_LY]	,
TargetTbl.[new_customer_transactions]	=SourceTbl.[new_customer_transactions]	,
TargetTbl.[new_customer_transactions_LY]	=SourceTbl.[new_customer_transactions_LY]	,
TargetTbl.[linked_transactions]	=SourceTbl.[linked_transactions]	,
TargetTbl.[linked_transactions_LY]	=SourceTbl.[linked_transactions_LY]	,
TargetTbl.[multi_category_transactions]	=SourceTbl.[multi_category_transactions]	,
TargetTbl.[multi_category_transactions_LY]	=SourceTbl.[multi_category_transactions_LY]	,
TargetTbl.[units]	=SourceTbl.[units]	,
TargetTbl.[units_LY]	=SourceTbl.[units_LY]	,
TargetTbl.[traffic]	=SourceTbl.[traffic]	,
TargetTbl.[traffic_LY]	=SourceTbl.[traffic_LY]	,
TargetTbl.[shopfront_traffic_open]	=SourceTbl.[shopfront_traffic_open]	,
TargetTbl.[shopfront_traffic_open_LY]	=SourceTbl.[shopfront_traffic_open_LY]	,
TargetTbl.[shopfront_traffic_closed]	=SourceTbl.[shopfront_traffic_closed]	,
TargetTbl.[shopfront_traffic_closed_LY]	=SourceTbl.[shopfront_traffic_closed_LY]	,
TargetTbl.[bounces60]	=SourceTbl.[bounces60]	,
TargetTbl.[bounces60_LY]	=SourceTbl.[bounces60_LY]	,
TargetTbl.[bounces120]	=SourceTbl.[bounces120]	,
TargetTbl.[bounces120_LY]	=SourceTbl.[bounces120_LY]	,
TargetTbl.[in_store_secs]	=SourceTbl.[in_store_secs]	,
TargetTbl.[in_store_secs_LY]	=SourceTbl.[in_store_secs_LY]	,
TargetTbl.[skincare_revenue_aud]	=SourceTbl.[skincare_revenue_aud]	,
TargetTbl.[skincare_revenue_aud_LY]	=SourceTbl.[skincare_revenue_aud_LY]	,
TargetTbl.[bodycare_revenue_aud]	=SourceTbl.[bodycare_revenue_aud]	,
TargetTbl.[bodycare_revenue_aud_LY]	=SourceTbl.[bodycare_revenue_aud_LY]	,
TargetTbl.[fragrance_revenue_aud]	=SourceTbl.[fragrance_revenue_aud]	,
TargetTbl.[fragrance_revenue_aud_LY]	=SourceTbl.[fragrance_revenue_aud_LY]	,
TargetTbl.[haircare_revenue_aud]	=SourceTbl.[haircare_revenue_aud]	,
TargetTbl.[haircare_revenue_aud_LY]	=SourceTbl.[haircare_revenue_aud_LY]	,
TargetTbl.[home_revenue_aud]	=SourceTbl.[home_revenue_aud]	,
TargetTbl.[home_revenue_aud_LY]	=SourceTbl.[home_revenue_aud_LY]	,
TargetTbl.[kits_revenue_aud]	=SourceTbl.[kits_revenue_aud]	,
TargetTbl.[kits_revenue_aud_LY]	=SourceTbl.[kits_revenue_aud_LY]	,
TargetTbl.[md_record_written_timestamp]	=SourceTbl.[md_record_written_timestamp]	,
TargetTbl.[md_record_written_pipeline_id]	= SourceTbl.[md_record_written_pipeline_id]	,
TargetTbl.[md_transformation_job_id]	=SourceTbl.[md_transformation_job_id]	
WHEN NOT MATCHED BY TARGET
THEN 
INSERT 
([storekpikey], [location_key], [date_key], [revenue_in_aud], [revenue_in_aud_LY], [target_aud], [target_aud_LY], [budget_aud], [budget_aud_LY], [transactions], [transactions_LY], [multi_unit_transactions], [multi_unit_transactions_LY], [new_customer_transactions], [new_customer_transactions_LY], [linked_transactions], [linked_transactions_LY], [multi_category_transactions], [multi_category_transactions_LY], [units], [units_LY], [traffic], [traffic_LY], [shopfront_traffic_open], [shopfront_traffic_open_LY], [shopfront_traffic_closed], [shopfront_traffic_closed_LY], [bounces60], [bounces60_LY], [bounces120], [bounces120_LY], [in_store_secs], [in_store_secs_LY], [skincare_revenue_aud], [skincare_revenue_aud_LY], [bodycare_revenue_aud], [bodycare_revenue_aud_LY], [fragrance_revenue_aud], [fragrance_revenue_aud_LY], [haircare_revenue_aud], [haircare_revenue_aud_LY], [home_revenue_aud], [home_revenue_aud_LY], [kits_revenue_aud], [kits_revenue_aud_LY], [md_record_written_timestamp], [md_record_written_pipeline_id], [md_transformation_job_id])
VALUES 
(SourceTbl.[storekpikey], SourceTbl.[location_key], SourceTbl.[date_key],SourceTbl.[revenue_in_aud], SourceTbl.[revenue_in_aud_LY], SourceTbl.[target_aud], SourceTbl.[target_aud_LY], SourceTbl.[budget_aud], SourceTbl.[budget_aud_LY], SourceTbl.[transactions], SourceTbl.[transactions_LY], SourceTbl.[multi_unit_transactions], SourceTbl.[multi_unit_transactions_LY], SourceTbl.[new_customer_transactions], SourceTbl.[new_customer_transactions_LY], SourceTbl.[linked_transactions], SourceTbl.[linked_transactions_LY], SourceTbl.[multi_category_transactions], SourceTbl.[multi_category_transactions_LY], SourceTbl.[units], SourceTbl.[units_LY], SourceTbl.[traffic], SourceTbl.[traffic_LY], SourceTbl.[shopfront_traffic_open], SourceTbl.[shopfront_traffic_open_LY], SourceTbl.[shopfront_traffic_closed], SourceTbl.[shopfront_traffic_closed_LY], SourceTbl.[bounces60], SourceTbl.[bounces60_LY], SourceTbl.[bounces120], SourceTbl.[bounces120_LY], SourceTbl.[in_store_secs], SourceTbl.[in_store_secs_LY], SourceTbl.[skincare_revenue_aud], SourceTbl.[skincare_revenue_aud_LY], SourceTbl.[bodycare_revenue_aud], SourceTbl.[bodycare_revenue_aud_LY], SourceTbl.[fragrance_revenue_aud], SourceTbl.[fragrance_revenue_aud_LY], SourceTbl.[haircare_revenue_aud], SourceTbl.[haircare_revenue_aud_LY], SourceTbl.[home_revenue_aud], SourceTbl.[home_revenue_aud_LY], SourceTbl.[kits_revenue_aud], SourceTbl.[kits_revenue_aud_LY], SourceTbl.[md_record_written_timestamp], SourceTbl.[md_record_written_pipeline_id], SourceTbl.[md_transformation_job_id])	;

/*BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT*/
DECLARE @label VARCHAR(500)
SET
  @label = 'AADPRETKPISTOKPIMNTHLY' EXEC meta_ctl.sp_row_count @jobid,
  @step_number,
  @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
  @newrec = max(md_record_written_timestamp)
FROM
  cons_retail.store_kpi_monthly;

SELECT
  @onlydate = CAST(@newrec AS DATE);

DELETE FROM
  cons_retail.store_kpi_monthly
WHERE
  md_record_written_timestamp = @newrec;

END
END TRY BEGIN CATCH /*ERROR OCCURED*/
PRINT 'ERROR SECTION INSERT'
INSERT
  meta_audit.transform_error_log_sp
SELECT
  ERROR_NUMBER() AS ErrorNumber,
  ERROR_SEVERITY() AS ErrorSeverity,
  ERROR_STATE() AS ErrorState,
  'cons_customer.sp_store_kpi_monthly' AS ErrorProcedure,
  ERROR_MESSAGE() AS ErrorMessage,
  getdate() AS Updated_date
END CATCH
END
