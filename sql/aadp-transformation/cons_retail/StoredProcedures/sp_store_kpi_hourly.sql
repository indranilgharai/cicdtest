/****** Object:  StoredProcedure [cons_retail].[sp_store_kpi_hourly]    Script Date: 1/24/2023 1:34:07 PM ******/
/****** Modified: Added logic to remove customergifts when products are selected        Script Date: 03/08/2023 3:39:00 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_store_kpi_hourly] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN 



DECLARE @batch_date [varchar](500)
select @batch_date=
	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(month,-4,getdate())
	else dateadd(month,-4,max(CAST(date_key as date))) end
from
	cons_retail.store_kpi_hourly;

IF OBJECT_ID('tempdb..#storekpihourly') IS NOT  NULL
BEGIN
    DROP TABLE #storekpihourly
END
create table #storekpihourly
with
(distribution=round_robin,
clustered index(storekpihourlykey)
)
as 



/**fetching the required attributes**/
SELECT
    concat(
        format(create_date_purchase, 'yyyyMMdd'),
        datepart(hour, create_date_purchase),
        location_code
    ) as storekpihourlykey,
    concat(
        format(create_date_purchase, 'yyyyMMdd'),
        location_code
    ) as storekpikey,
    location_code as location_key,
    create_date_purchase as date_key,
    datepart(hour, create_date_purchase) as purchase_hour,
    transactions,
    multi_unit_transactions,
    new_customer_transactions,
    linked_transactions,
    multi_category_transactions,
    units,
    traffic,
    shopfront_traffic_open,
    shopfront_traffic_closed,
    bounces60,
	bounces120,
    in_store_secs,
    getdate() as md_record_written_timestamp,
    @pipelineid as md_record_written_pipeline_id,
    @jobid as md_transformation_job_id
FROM
    (
        SELECT
            location_code,
            create_date_purchase,
            COUNT(distinct ORDERid) transactions,
			/**logic for multi_unit_transactions**/
            SUM(
                CASE
                    WHEN units > 1 THEN 1
                    ELSE 0
                END
            ) multi_unit_transactions,
			/**logic for new_customer_transactions**/
            SUM(
                CASE
                    WHEN new_customer = 'Y' THEN 1
                    ELSE 0
                END
            ) new_customer_transactions,
            SUM(linked_transactions) linked_transactions,
			/**logic for multi_category_transactions**/
            SUM(
                CASE
                    WHEN category > 1 THEN 1
                    ELSE 0
                END
            ) multi_category_transactions,
            SUM(units) units
        FROM
            (
			/*fetching required columns at ORDERid,location_code,create_date_purchase,new_customer,linked_transactions*/
                SELECT
                    ORDERid,
                    location_code,
                    create_date_purchase,
                    SUM(sales_units) AS units,
                    new_customer,
                    linked_transactions,
                    COUNT(distinct category) category
                FROM
                    (
                        SELECT
                            DISTINCT 
							/*logic to find location_code*/
                            case
                                when order_type = 'ClickandCollect' 
                                THEN (
                                    CASE
                                        WHEN pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                        WHEN pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                        WHEN pr.source_system = 'HYBRIS' THEN CAST(
                                            ISNULL(
                                                pr.fulfillment_location_code,
                                                ISNULL(pr.location_code, '999')
                                            ) AS VARCHAR(50)
                                        )
                                        ELSE NULL
                                    END
                                )
                                ELSE (
                                    CASE
                                        WHEN pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                        WHEN pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                        WHEN pr.source_system = 'HYBRIS' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)
                                        ELSE NULL
                                    END
                                )
                            END location_code,
                            /*logic to find create_date_purchase*/
                            dateadd(
                                hour,
                                datediff(
                                    hour,
                                    0,
                                    cast(
                                        case
                                            when (pr.channel_id = 'Digital' )
                                             then case
                                                when left(pr.orderid, 1) = 'H' then CASE
                                                    WHEN pr.source_system = 'HYBRIS'
                                                    AND (
                                                        pr.OrderStatus = 'SHIPPED'
                                                        or pr.OrderStatus = 'DELIVERED'
                                                        or pr.OrderStatus = 'COMPLETED'
                                                        or pr.OrderStatus = 'RETURNED'
                                                    ) THEN shipped_date
                                                    ELSE NULL
                                                END
                                                else pr.create_date_purchase
                                            end
                                            else pr.create_date_purchase
                                        end as datetime
                                    )
                                ),
                                0
                            ) create_date_purchase,
                            
                            sales_units,
                            /*logic to find Linked_transactions*/
                            (
                                CASE
                                    WHEN ISNULL(pr.Customer_ID, '') = '' THEN 0
                                    WHEN pr.Customer_ID like 'WI00%' THEN 0
                                    ELSE 1
                                END
                            ) AS Linked_transactions,
                            prd.category,
                            pr.ORDERid,
                            retail_transaction_line_itemid,
                            /*logic to find new_customer*/
                            ISNULL(
                                (
                                    CASE
                                        WHEN cast(pr.create_date_purchase AS DATE) <= cast(
                                            dateadd(month, 3, f_date.First_purchase_date) AS DATE
                                        ) THEN 'Y'
                                        ELSE 'N'
                                    END
                                ),
                                'N'
                            ) AS new_customer
                        FROM
                          (select * from  std.purchase_record WHERE cast(
					case
						when (channel_id = 'Digital' )
						then case
							when left(orderid, 1) = 'H' then CASE
								WHEN source_system = 'HYBRIS'
								AND (
									OrderStatus = 'SHIPPED'
									or OrderStatus = 'DELIVERED'
									or OrderStatus = 'COMPLETED'
									or OrderStatus = 'RETURNED'
								) THEN shipped_date
								ELSE NULL
							END
							else create_date_purchase
						end
						else create_date_purchase
					end as date
				) >=@batch_date) pr
							/*subquery with logic to fetch sales_units,product_code from purchase_record_line_item*/
                            INNER JOIN (
                                SELECT
                                    distinct ORDERid,
                                    retail_transaction_line_itemid,
									/*logic to find sales_units*/
                                    case
                                        when sample_flag = 'Y' then 0
                                        when source_system = 'HYBRIS'
                                        and return_flag = 'Y' then (abs(sales_units) - return_qty)
                                        when source_system = 'HYBRIS'
                                        and cancelled_flag = 'Y' then (abs(sales_units) - cancellation_qty)
                                        when source_system = 'HYBRIS'
                                        and (
                                            return_flag = 'Y'
                                            and cancelled_flag = 'Y'
                                        ) then (abs(sales_units) - return_qty - cancellation_qty)
                                        when source_system = 'HYBRIS' then abs(sales_units)
                                        else sales_units
                                    end sales_units,
                                    product_code 
                                FROM
                                    std.purchase_record_line_item
								/*logic to fetch non-cancelled orders*/
                                WHERE
                                    (
                                        UPPER(cancelled_flag) IN ('N')
                                        OR cancelled_flag IS NULL
                                    )
								/*logic to neglect orders with CLICKCOLLECT product_code*/
                                    and orderid not in (
                                        select
                                            orderid
                                        from
                                            std.purchase_record_line_item
                                        where
                                            product_code = 'CLICKCOLLECT'
                                    )
                                    
                            ) prli ON pr.ORDERid = prli.ORDERid
                            /*logic to fetch records based on specific product_type_sub_cat and category*/ 
                            INNER JOIN (
                                select
                                    *
                                from
                                    std.product_x
                                where
                                    product_type_sub_cat in ('Retail', 'Kit Item Only')
                                    and category not in ('Non Sale', 'Packaging Component', 'Voucher')
                                    and description1 not in ('CUSTOMERGIFT') /* to remove customergifts*/
                            ) prd on prli.product_code = prd.description1
							/* logic to find first purchase date at customerid level*/
                            LEFT JOIN (
                                SELECT
                                    customer_id,
                                    create_date_purchase AS First_purchase_date
                                FROM
                                    (
                                        SELECT
                                            row_number() OVER(
                                                PARTITION BY customer_id
                                                ORDER BY
                                                    create_date_purchase ASC
                                            ) rowval,
                                            *
                                        FROM
                                            (
                                                SELECT
                                                    DISTINCT customer_id,
                                                    create_date_purchase
                                                FROM
                                                    std.purchase_record WITH (NOLOCK)
                                                where
                                                    customer_id <> ''
                                            ) pur
                                    ) a
                                WHERE
                                    a.rowval = 1
                            ) f_date ON pr.customer_id = f_date.customer_id 
                            
                    ) main
                GROUP BY
                    ORDERid,
                    location_code,
                    create_date_purchase,
                    new_customer,
                    linked_transactions
            ) outerq
        GROUP BY
            location_code,
            create_date_purchase 
    ) outerq2
	/*logic to calculate traffic,shopfront_traffic_open,shopfront_traffic_closed,shopfront_conversion,bounces amd in_store_secs*/
    LEFT JOIN (
        SELECT
            FORMAT(CAST(sbs_no AS INT), '00', 'en-US') + FORMAT(CAST(store_no AS INT), '000', 'en-US') locationcode,
            CONVERT(date, CAST(traffic_date AS VARCHAR)) traffic_date,
            traffic_hour,
            SUM(inside) AS traffic,
            SUM(outside_traffic_during_work_hours) shopfront_traffic_open,
            SUM(outside_traffic_outside_work_hours) shopfront_traffic_closed,
            SUM(shopfront_conversion) shopfront_conversion,
            SUM(br60_qty) bounces60,
			SUM(br120_qty) bounces120,
            SUM(dwell_time_total_seconds) in_store_secs
        FROM
            std.kepler_incoming
			where CONVERT(date, CAST(traffic_date AS VARCHAR))>=@batch_date
        GROUP BY
            sbs_no,
            store_no,
            CONVERT(date, CAST(traffic_date AS VARCHAR)),
            traffic_hour
    ) traffic ON location_code = traffic.locationcode
    and cast(create_date_purchase as date) = traffic_date
    and traffic_hour = datepart(hour, create_date_purchase) 
    
    OPTION (LABEL = 'AADPRETKPISTOKPIHRLY');

	
MERGE
INTO cons_retail.store_kpi_hourly as TargetTbl
USING #storekpihourly as SourceTbl
ON  SourceTbl.storekpihourlykey= TargetTbl.storekpihourlykey
WHEN MATCHED 
THEN UPDATE SET
TargetTbl.[storekpihourlykey]	=SourceTbl.[storekpihourlykey]	,
TargetTbl.[storekpikey]	=SourceTbl.[storekpikey]	,
TargetTbl.[location_key]	=SourceTbl.[location_key]	,
TargetTbl.[date_key]	=SourceTbl.[date_key]	,
TargetTbl.[purchase_hour]	=SourceTbl.[purchase_hour]	,
TargetTbl.[transactions]	=SourceTbl.[transactions]	,
TargetTbl.[multi_unit_transactions]	=SourceTbl.[multi_unit_transactions]	,
TargetTbl.[new_customer_transactions]	=SourceTbl.[new_customer_transactions]	,
TargetTbl.[linked_transactions]	=SourceTbl.[linked_transactions]	,
TargetTbl.[multi_category_transactions]	=SourceTbl.[multi_category_transactions]	,
TargetTbl.[units]	=SourceTbl.[units]	,
TargetTbl.[traffic]	=SourceTbl.[traffic]	,
TargetTbl.[shopfront_traffic_open]	=SourceTbl.[shopfront_traffic_open]	,
TargetTbl.[shopfront_traffic_closed]	=SourceTbl.[shopfront_traffic_closed]	,
TargetTbl.[bounces60]	=SourceTbl.[bounces60]	,
TargetTbl.[bounces120]	=SourceTbl.[bounces120]	,
TargetTbl.[in_store_secs]	=SourceTbl.[in_store_secs]	,
TargetTbl.[md_record_written_timestamp]	=SourceTbl.[md_record_written_timestamp]	,
TargetTbl.[md_record_written_pipeline_id]	= SourceTbl.[md_record_written_pipeline_id]	,
TargetTbl.[md_transformation_job_id]	=SourceTbl.[md_transformation_job_id]	
WHEN NOT MATCHED BY TARGET
THEN 
INSERT 
(
[storekpihourlykey]
      ,[storekpikey]
      ,[location_key]
      ,[date_key]
      ,[purchase_hour]
      ,[transactions]
      ,[multi_unit_transactions]
      ,[new_customer_transactions]
      ,[linked_transactions]
      ,[multi_category_transactions]
      ,[units]
      ,[traffic]
      ,[shopfront_traffic_open]
      ,[shopfront_traffic_closed]
      ,[bounces60]
      ,[bounces120]
      ,[in_store_secs]
      ,[md_record_written_timestamp]
      ,[md_record_written_pipeline_id]
      ,[md_transformation_job_id]
	  )
VALUES 
(SourceTbl.[storekpihourlykey]	,
SourceTbl.[storekpikey]	,
SourceTbl.[location_key]	,
SourceTbl.[date_key]	,
SourceTbl.[purchase_hour]	,
SourceTbl.[transactions]	,
SourceTbl.[multi_unit_transactions]	,
SourceTbl.[new_customer_transactions]	,
SourceTbl.[linked_transactions]	,
SourceTbl.[multi_category_transactions]	,
SourceTbl.[units]	,
SourceTbl.[traffic]	,
SourceTbl.[shopfront_traffic_open]	,
SourceTbl.[shopfront_traffic_closed]	,
SourceTbl.[bounces60]	,
SourceTbl.[bounces120]	,
SourceTbl.[in_store_secs]	,
SourceTbl.[md_record_written_timestamp]	,
SourceTbl.[md_record_written_pipeline_id]	,
SourceTbl.[md_transformation_job_id]	)	;



/*BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT*/
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPRETKPISTOKPIHRLY' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = max(md_record_written_timestamp)
FROM
    cons_retail.store_kpi_hourly;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    cons_retail.store_kpi_hourly
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
    'cons_customer.sp_store_kpi_hourly' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
