/****** Object:  StoredProcedure [cons_retail].[sp_store_consultant_kpi_monthly]    Script Date: 12/20/2022 1:49:16 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_store_consultant_kpi_monthly] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN 

DECLARE @batch_date [varchar](500)
select @batch_date=
	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(month,-4,getdate())
	else dateadd(month,-4,max(CAST(date_key as date))) end
from
	cons_retail.store_consultant_kpi_monthly;


IF OBJECT_ID('tempdb..#storeconsultantmonthly') IS NOT  NULL
BEGIN
    DROP TABLE #storeconsultantmonthly
END
create table #storeconsultantmonthly
with
(distribution=round_robin,
clustered index(storekpiconsultantkey)
)
as 




/*****aggregating store_consultant_kpi data at month level*****/
SELECT
    CAST(
        concat(
            format(DATEADD(DAY, 1, EOMONTH(date_key, -1)), 'yyyyMMdd'),
            location_key,
            consultant_key
        ) AS varchar(250)
    ) AS storekpiconsultantkey,
    CAST(
        concat(
            format(DATEADD(DAY, 1, EOMONTH(date_key, -1)), 'yyyyMMdd'),
            location_key
        ) AS varchar(250)
    ) AS storekpikey,
    [consultant_key] as consultant_key,
    [location_key] as location_key,
    DATEADD(DAY, 1, EOMONTH(date_key, -1)) date_key,
    sum([revenue_in_aud]) as revenue_in_aud,
    sum([transactions]) as transactions,
    sum([linked_transactions]) as linked_transactions,
    sum([multi_unit_transactions]) as multi_unit_transactions,
    sum([multi_category_transactions]) as multi_category_transactions,
    sum([units]) as units,
    sum([skincare_revenue_aud]) as skincare_revenue_aud,
    sum([bodycare_revenue_aud]) as bodycare_revenue_aud,
    sum([fragrance_revenue_aud]) as fragrance_revenue_aud,
    sum([haircare_revenue_aud]) as haircare_revenue_aud,
    sum([home_revenue_aud]) as home_revenue_aud,
    sum([kits_revenue_aud]) as kits_revenue_aud,
    getdate() as md_record_written_timestamp,
    @pipelineid as md_record_written_pipeline_id,
    @jobid as md_transformation_job_id
FROM
    [cons_retail].[store_consultant_kpi_daily]
	where date_key>=DATEADD(DAY, 1, EOMONTH(@batch_date, -1))
GROUP BY
    consultant_key,
    location_key,
    DATEADD(DAY, 1, EOMONTH(date_key, -1)) OPTION (LABEL = 'AADPRETKPICONSMON');


MERGE
INTO cons_retail.store_consultant_kpi_monthly as TargetTbl
USING #storeconsultantmonthly as SourceTbl
ON  SourceTbl.storekpiconsultantkey= TargetTbl.storekpiconsultantkey
WHEN MATCHED 
THEN UPDATE 
SET
TargetTbl.[storekpiconsultantkey]	=SourceTbl.[storekpiconsultantkey],
TargetTbl.[storekpikey]	=SourceTbl.[storekpikey]	,
TargetTbl.[location_key]=SourceTbl.[location_key]	,
TargetTbl.[consultant_key]=SourceTbl.[consultant_key]	,
TargetTbl.[date_key]=SourceTbl.[date_key]	,
TargetTbl.[revenue_in_aud]=SourceTbl.[revenue_in_aud]	,
TargetTbl.[transactions]=SourceTbl.[transactions]	,
TargetTbl.[linked_transactions]=SourceTbl.[linked_transactions]	,
TargetTbl.[multi_unit_transactions]=SourceTbl.[multi_unit_transactions]	,
TargetTbl.[multi_category_transactions]=SourceTbl.[multi_category_transactions]	,
TargetTbl.[units]=SourceTbl.[units]	,
TargetTbl.[skincare_revenue_aud]=SourceTbl.[skincare_revenue_aud]	,
TargetTbl.[bodycare_revenue_aud]=SourceTbl.[bodycare_revenue_aud]	,
TargetTbl.[fragrance_revenue_aud]=SourceTbl.[fragrance_revenue_aud]	,
TargetTbl.[haircare_revenue_aud]=SourceTbl.[haircare_revenue_aud]	,
TargetTbl.[home_revenue_aud]=SourceTbl.[home_revenue_aud]	,
TargetTbl.[kits_revenue_aud]=SourceTbl.[kits_revenue_aud]	,
TargetTbl.[md_record_written_timestamp]=SourceTbl.[md_record_written_timestamp]	,
TargetTbl.[md_record_written_pipeline_id]= SourceTbl.[md_record_written_pipeline_id]	,
TargetTbl.[md_transformation_job_id]=SourceTbl.[md_transformation_job_id]	
WHEN NOT MATCHED BY TARGET
THEN INSERT 
VALUES 
(
SourceTbl.[storekpiconsultantkey],
SourceTbl.[storekpikey]	,
SourceTbl.[location_key]	,
SourceTbl.[consultant_key]	,
SourceTbl.[date_key]	,
SourceTbl.[revenue_in_aud]	,
SourceTbl.[transactions]	,
SourceTbl.[linked_transactions]	,
SourceTbl.[multi_unit_transactions]	,
SourceTbl.[multi_category_transactions]	,
SourceTbl.[units]	,
SourceTbl.[skincare_revenue_aud]	,
SourceTbl.[bodycare_revenue_aud]	,
SourceTbl.[fragrance_revenue_aud]	,
SourceTbl.[haircare_revenue_aud]	,
SourceTbl.[home_revenue_aud]	,
SourceTbl.[kits_revenue_aud]	,
SourceTbl.[md_record_written_timestamp]	,
SourceTbl.[md_record_written_pipeline_id]	,
SourceTbl.[md_transformation_job_id]	
)
;


/*BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT*/
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPRETKPICONSMON' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = max(md_record_written_timestamp)
FROM
    cons_retail.store_consultant_kpi_monthly;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    cons_retail.store_consultant_kpi_monthly
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
    'cons_customer.sp_store_consultant_kpi_monthly' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
