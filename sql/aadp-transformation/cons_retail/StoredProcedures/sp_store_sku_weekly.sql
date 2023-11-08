/****** Object:  StoredProcedure [cons_retail].[sp_store_sku_weekly]    Script Date: 12/20/2022 1:53:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_store_sku_weekly] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN


DECLARE @batch_date [varchar](500)
select @batch_date=
	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(month,-4,getdate())
	else dateadd(month,-4,max(CAST(date_key as date))) end
	FROM
	cons_retail.store_sku_weekly;

--truncate table cons_retail.store_sku_daily;
/***base_query cte - to get required revenue details,date_code,product_key,units,location_code etc ***/

IF OBJECT_ID('tempdb..#storeskuweekly') IS NOT  NULL
BEGIN
    DROP TABLE #storeskuweekly
END
create table #storeskuweekly
with
(distribution=round_robin,
clustered index(storeskukey)
)
as 


/*****aggregating store_sku data at week level*****/
SELECT
    CAST(
        concat(
            format(
                cast(
                    DATEADD(
                        d,
                        - cast((DATEPART(WEEKDAY, date_key)) as int) + 1,
                        date_key
                    ) as date
                ),
                'yyyyMMdd'
            ),
            location_key,
            product_key
        ) AS varchar(250)
    ) as [storeskukey],
    CAST(
        concat(
            format(
                cast(
                    DATEADD(
                        d,
                        - cast((DATEPART(WEEKDAY, date_key)) as int) + 1,
                        date_key
                    ) as date
                ),
                'yyyyMMdd'
            ),
            location_key
        ) AS varchar(250)
    ) [storekpikey],
    cast(
        DATEADD(
            d,
            - cast((DATEPART(WEEKDAY, date_key)) as int) + 1,
            date_key
        ) as date
    ) as [date_key],
    [location_key],
    [product_key],
    sum([units]) as [units],
    sum([units_LY]) as [units_LY],
    sum([revenue_in_aud]) as [revenue_in_aud],
    sum([revenue_in_aud_LY]) as [revenue_in_aud_LY],
    getdate() as md_record_written_timestamp,
    @pipelineid as md_record_written_pipeline_id,
    @jobid as md_transformation_job_id
FROM
    [cons_retail].[store_sku_daily]
	where date_key>= cast(
        DATEADD(
            d,
            - cast((DATEPART(WEEKDAY, @batch_date)) as int) + 1,
            @batch_date
        ) as date)
group by
    [location_key],
    [product_key],
    cast(
        DATEADD(
            d,
            - cast((DATEPART(WEEKDAY, date_key)) as int) + 1,
            date_key
        ) as date
    ) OPTION (LABEL = 'AADPRETKPISTOSKUWKLY');


MERGE
INTO cons_retail.store_sku_weekly as TargetTbl
USING #storeskuweekly as SourceTbl
ON  SourceTbl.storeskukey= TargetTbl.storeskukey
WHEN MATCHED 
THEN UPDATE 
SET
TargetTbl.[storeskukey]	=SourceTbl.[storeskukey]	,
TargetTbl.[storekpikey]	=SourceTbl.[storekpikey]	,
TargetTbl.[location_key]=SourceTbl.[location_key]	,
TargetTbl.[product_key]=SourceTbl.[product_key]	,
TargetTbl.[date_key]=SourceTbl.[date_key]	,
TargetTbl.[revenue_in_aud]=SourceTbl.[revenue_in_aud]	,
TargetTbl.[revenue_in_aud_LY]=SourceTbl.[revenue_in_aud_LY]	,
TargetTbl.[units]=SourceTbl.[units]	,
TargetTbl.[units_LY]=SourceTbl.[units_LY]	,
TargetTbl.[md_record_written_timestamp]=SourceTbl.[md_record_written_timestamp]	,
TargetTbl.[md_record_written_pipeline_id]= SourceTbl.[md_record_written_pipeline_id]	,
TargetTbl.[md_transformation_job_id]=SourceTbl.[md_transformation_job_id]	
WHEN NOT MATCHED BY TARGET
THEN INSERT 
([storeskukey],
[storekpikey],
[date_key],
[location_key],[product_key],
[units],
[units_LY],
[revenue_in_aud],
[revenue_in_aud_LY],
[md_record_written_timestamp],
[md_record_written_pipeline_id],
[md_transformation_job_id])
VALUES 
(SourceTbl.[storeskukey], 
SourceTbl.[storekpikey],
SourceTbl.[date_key], SourceTbl.[location_key], SourceTbl.[product_key], SourceTbl.[units], SourceTbl.[units_LY], SourceTbl.[revenue_in_aud], SourceTbl.[revenue_in_aud_LY], SourceTbl.[md_record_written_timestamp], SourceTbl.[md_record_written_pipeline_id], SourceTbl.[md_transformation_job_id])
;




--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPRETKPISTOSKUWKLY' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = max(md_record_written_timestamp)
FROM
    cons_retail.store_sku_weekly;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    cons_retail.store_sku_weekly
WHERE
    md_record_written_timestamp = @newrec;

END
END TRY BEGIN CATCH --ERROR OCCURED
PRINT 'ERROR SECTION INSERT'
INSERT
    meta_audit.transform_error_log_sp
SELECT
    ERROR_NUMBER() AS ErrorNumber,
    ERROR_SEVERITY() AS ErrorSeverity,
    ERROR_STATE() AS ErrorState,
    'cons_customer.sp_store_sku_weekly' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
