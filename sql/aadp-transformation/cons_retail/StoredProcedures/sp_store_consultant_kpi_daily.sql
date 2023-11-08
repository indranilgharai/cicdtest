/****** Object:  StoredProcedure [cons_retail].[sp_store_consultant_kpi_daily]    Script Date: 1/24/2023 1:32:54 PM ******/
/****** Modified: added delete logic in  StoredProcedure [cons_retail].[sp_store_consultant_kpi_daily]    Script Date: 2/23/2023 3:32:54 PM ******/
/****** Modified: Modified delete logic and removed from DELETE Statement in  StoredProcedure [cons_retail].[sp_store_consultant_kpi_daily]    Script Date: 2/24/2023 3:59:54 PM ******/
/****** Modified: Added logic to remove customergifts when products are selected        Script Date: 03/08/2023 3:39:00 PM ******/
/****** Modified: Added bundle_sku_line_no to inner join to ensure correct granularity    Script Date: 10/19/2023 13:00:00 PM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_store_consultant_kpi_daily] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN

DECLARE @batch_date [varchar](500)
select @batch_date=
case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(month,-4,getdate())
	else dateadd(month,-4,max(CAST(date_key as date))) end
from
	cons_retail.store_consultant_kpi_daily;

--truncate table cons_retail.store_sku_daily;
/***base_query cte - to get required revenue details,date_code,product_key,units,location_code etc ***/

IF OBJECT_ID('tempdb..#storeconsultantdaily') IS NOT  NULL
BEGIN
    DROP TABLE #storeconsultantdaily
END
create table #storeconsultantdaily
with
(distribution=round_robin,
clustered index(storekpiconsultantkey)
)
as 


/***store_consultant_con cte - to get revenue details, employeecode,location_code etc at employeecode,location_code level,create_date_purchase***/

WITH store_consultant_con AS (
    SELECT
        employeecode,
        location_code,
        create_date_purchase,
        SUM(revenue_in_aud) revenue_in_aud,
        COUNT(DISTINCT orderid) transactions,
        SUM(linked_transactions) linked_transactions,
        SUM(
            CASE
                WHEN units > 1 THEN 1
                ELSE 0
            end
        ) multi_unit_transactions,
        SUM(
            CASE
                WHEN category > 1 THEN 1
                ELSE 0
            end
        ) multi_category_transactions,
        SUM(units) units,
        SUM(skincare_revenue_aud) AS skincare_revenue_aud,
        SUM(bodycare_revenue_aud) AS bodycare_revenue_aud,
        SUM(fragrance_revenue_aud) AS fragrance_revenue_aud,
        SUM(haircare_revenue_aud) AS haircare_revenue_aud,
        SUM(home_revenue_aud) AS home_revenue_aud,
        SUM(kits_revenue_aud) AS kits_revenue_aud
    FROM
        (
			/*subquery to get aggregated revenue values at orderid,employeecode,created_purchase_date level*/
            SELECT
                employeecode,
				location_code,
                orderid,
                create_date_purchase,
                SUM(revenue_in_aud) revenue_in_aud,
                SUM(sales_units) AS units,
                linked_transactions,
                COUNT(DISTINCT category) category,
				
				/******aggregated revenues based on category******/
                SUM(skincare_revenue_aud) AS skincare_revenue_aud,
                SUM(bodycare_revenue_aud) AS bodycare_revenue_aud,
                SUM(fragrance_revenue_aud) AS fragrance_revenue_aud,
                SUM(haircare_revenue_aud) AS haircare_revenue_aud,
                SUM(home_revenue_aud) AS home_revenue_aud,
                SUM(kits_revenue_aud) AS kits_revenue_aud
            FROM
                (
					/*fetching required columns at line item level*/
                    SELECT
                        DISTINCT case when trim(sales_consultant ) ='' then NULL else sales_consultant  end  employeecode,
                        case
                            when order_type = 'ClickandCollect' --then fulfillment store else origin store end 
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
                        CAST(
                            CASE
                                WHEN (pr.channel_id = 'Digital')
                                THEN CASE
                                    WHEN left(pr.orderid, 1) = 'H' THEN CASE
                                        WHEN pr.source_system = 'HYBRIS'
                                        AND (
                                            pr.OrderStatus = 'SHIPPED'
                                            or pr.OrderStatus = 'DELIVERED'
                                            or pr.OrderStatus = 'COMPLETED'
                                            or pr.OrderStatus = 'RETURNED'
                                        ) THEN shipped_date
                                        ELSE NULL
                                    END
                                    ELSE pr.create_date_purchase
                                END
                                ELSE pr.create_date_purchase
                            END AS DATE
                        ) create_date_purchase,
                        retail_transaction_line_itemid,
                        bundle_sku_line_no,
						/*logic to find revenue_in_AUD*/
                        CASE
                            WHEN pr.currency_code = 'AUD' THEN prli.price
                            ELSE (
                                CAST(prli.price AS float) / CAST(exrate.ex_rate as FLOAT)
                            )
                        END AS revenue_in_AUD,
                        sales_units,
                        (
                            CASE
                                WHEN ISNULL(pr.Customer_ID, '') = '' THEN 0
                                WHEN pr.Customer_ID like 'WI00%' THEN 0
                                ELSE 1
                            END
                        ) AS Linked_transactions,
                        prd.category,
                        CASE
                            WHEN pr.currency_code = 'AUD' THEN prli.price
                            ELSE (
                                CAST(prli.price AS float) / CAST(exrate.ex_rate AS FLOAT)
                            )
                        END price,
						
						/************************category based revenue************************/
						/*Revenue logic of category: SKIN CARE*/
                        CASE
                            WHEN prd.category = 'SKIN CARE' THEN (
                                CASE
                                    WHEN pr.currency_code = 'AUD' THEN prli.price
                                    ELSE (
                                        CAST(prli.price AS float) / CAST(exrate.ex_rate AS FLOAT)
                                    )
                                END
                            )
                        end AS skincare_revenue_aud,
						/*Revenue logic of category: BODY CARE*/
                        CASE
                            WHEN prd.category = 'BODY CARE' THEN (
                                CASE
                                    WHEN pr.currency_code = 'AUD' THEN prli.price
                                    ELSE (
                                        CAST(prli.price AS float) / CAST(exrate.ex_rate AS FLOAT)
                                    )
                                END
                            )
                        end AS bodycare_revenue_aud,
						/*Revenue logic of category: FRAGRANCE*/
                        CASE
                            WHEN prd.category = 'FRAGRANCE' THEN (
                                CASE
                                    WHEN pr.currency_code = 'AUD' THEN prli.price
                                    ELSE (
                                        CAST(prli.price AS float) / CAST(exrate.ex_rate AS FLOAT)
                                    )
                                END
                            )
                        end AS fragrance_revenue_aud,
						/*Revenue logic of category: HAIR/HAIR CARE*/
                        CASE
                            WHEN prd.category IN ('HAIR', 'HAIR CARE') THEN (
                                CASE
                                    WHEN pr.currency_code = 'AUD' THEN prli.price
                                    ELSE (
                                        CAST(prli.price AS float) / CAST(exrate.ex_rate AS FLOAT)
                                    )
                                END
                            )
                        end AS haircare_revenue_aud,
						/*Revenue logic of category: HOME*/
                        CASE
                            WHEN prd.category = 'HOME' THEN (
                                CASE
                                    WHEN pr.currency_code = 'AUD' THEN prli.price
                                    ELSE (
                                        CAST(prli.price AS float) / CAST(exrate.ex_rate AS FLOAT)
                                    )
                                END
                            )
                        end AS home_revenue_aud,
						/*Revenue logic of category: KITS*/
                        CASE
                            WHEN prd.category = 'KITS' THEN (
                                CASE
                                    WHEN pr.currency_code = 'AUD' THEN prli.price
                                    ELSE (
                                        CAST(prli.price AS float) / CAST(exrate.ex_rate AS FLOAT)
                                    )
                                END
                            )
                        end AS kits_revenue_aud,
                        exrate.ex_rate,
                        pr.orderid
                    FROM
			(select * from 	std.purchase_record where cast(
					case
						when (channel_id = 'Digital')
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
				) >=@batch_date)pr
                        LEFT JOIN (
                            SELECT
                                *
                            FROM
                                (
                                    SELECT
                                        sbs_no,
                                        ex_rate,
                                        year,
                                        month_no,
                                        ROW_NUMBER() OVER(
                                            PARTITION BY sbs_no
                                            order BY
                                                year DESC,
                                                month_no DESC
                                        ) rwno
                                    FROM
                                        [std].[exchange_rate_x]
                                ) a
                            WHERE
                                rwno = 1
                        ) exrate ON CAST(pr.storx_sbs_no AS INT) = CAST(exrate.sbs_no AS INT)
						
						/*subquery with logic to fetch sales_units,price,product_code from purchase_record_line_item*/
                        INNER JOIN (
                            SELECT
                                distinct ORDERid,
                                retail_transaction_line_itemid,
								bundle_sku_line_no,
								/*logic to find sales_units*/
                                CASE
                                    WHEN sample_flag = 'Y' THEN 0
                                    WHEN source_system = 'HYBRIS'
                                    AND return_flag = 'Y' THEN (ABS(sales_units) - return_qty)
                                    WHEN source_system = 'HYBRIS'
                                    AND cancelled_flag = 'Y' THEN (ABS(sales_units) - cancellation_qty)
                                    WHEN source_system = 'HYBRIS'
                                    AND (
                                        return_flag = 'Y'
                                        AND cancelled_flag = 'Y'
                                    ) THEN (ABS(sales_units) - return_qty - cancellation_qty)
                                    WHEN source_system = 'HYBRIS' THEN ABS(sales_units)
                                    ELSE sales_units
                                END sales_units,
								
								/*logic to find price*/
								CASE
                                    WHEN source_system = 'HYBRIS'
                                    AND return_flag = 'Y' THEN (revenue_tax_exc_local - return_value)
                                    WHEN source_system = 'HYBRIS'
                                    AND cancelled_flag = 'Y' THEN (revenue_tax_exc_local - cancellation_value)
                                    WHEN source_system = 'HYBRIS'
                                    AND (
                                        return_flag = 'Y'
                                        AND cancelled_flag = 'Y'
                                    ) THEN (
                                        revenue_tax_exc_local - return_value - cancellation_value
                                    )
                                    ELSE revenue_tax_exc_local
                                END AS price,
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
                        ) prli ON pr.orderid = prli.orderid
						
						/*logic to fetch records based on specific product_type_sub_cat and category*/ 
                        INNER JOIN (
                            SELECT
                                *
                            FROM
                                std.product_x
                            WHERE
                                product_type_sub_cat in ('Retail', 'Kit Item Only')
                                and category not in ('Non Sale', 'Packaging Component', 'Voucher')
                                and description1 not in ('CUSTOMERGIFT') /* To remove customergifts*/
                        ) prd on prli.product_code = prd.description1
                ) main
            GROUP BY
                employeecode,
                location_code,
                create_date_purchase,
                orderid,
                linked_transactions 
        ) outerq
    GROUP BY
        employeecode,
        location_code,
        create_date_purchase
)

/*Inserting data to target table ([cons_retail].[store_consultant_kpi_daily])*/
--INSERT INTO [cons_retail].[store_consultant_kpi_daily]
SELECT
    CAST(
        concat(
            format(create_date_purchase, 'yyyyMMdd'),
            location_code,
            employeecode
        ) AS varchar(250)
    ) AS storekpiconsultantkey,
    CAST(
        concat(
            format(create_date_purchase, 'yyyyMMdd'),
            location_code
        ) AS varchar(250)
    ) AS storekpikey,
    CAST(employeecode AS varchar(10)) AS consultant_key,
    CAST(location_code AS varchar(10)) location_key,
    CAST(create_date_purchase as varchar(50)) AS date_key,
    CAST(revenue_in_aud AS float) revenue_in_aud,
    CAST(transactions AS int) transactions,
    CAST(linked_transactions AS int) linked_transactions,
    CAST(multi_unit_transactions AS int) multi_unit_transactions,
    CAST(multi_category_transactions AS int) multi_category_transactions,
    CAST(units AS int) units,
    CAST(skincare_revenue_aud AS float) skincare_revenue_aud,
    CAST(bodycare_revenue_aud AS float) bodycare_revenue_aud,
    CAST(fragrance_revenue_aud AS float) fragrance_revenue_aud,
    CAST(haircare_revenue_aud AS float) haircare_revenue_aud,
    CAST(home_revenue_aud AS float) home_revenue_aud,
    CAST(kits_revenue_aud AS float) kits_revenue_aud,
    GETDATE() AS md_record_written_timestamp,
    @pipelineid AS md_record_written_pipeline_id,
    @jobid AS md_transformation_job_id
FROM
    store_consultant_con OPTION (LABEL = 'AADPRETKPICONSDLY');

-- Added CTE and DELETE to handle null values for consultant id from cegid streaming    

DELETE FROM [cons_retail].[store_consultant_kpi_daily]
where  date_key > @batch_date and consultant_key is NULL

MERGE
INTO [cons_retail].[store_consultant_kpi_daily] as TargetTbl
USING #storeconsultantdaily as SourceTbl
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
(
[storekpiconsultantkey],
[storekpikey]	,
[location_key]	,
[consultant_key]	,
[date_key]	,
[revenue_in_aud]	,
[transactions]	,
[linked_transactions]	,
[multi_unit_transactions]	,
[multi_category_transactions]	,
[units]	,
[skincare_revenue_aud]	,
[bodycare_revenue_aud]	,
[fragrance_revenue_aud]	,
[haircare_revenue_aud]	,
[home_revenue_aud]	,
[kits_revenue_aud]	,
[md_record_written_timestamp]	,
[md_record_written_pipeline_id]	,
[md_transformation_job_id]	
)
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
    @label = 'AADPRETKPICONSDLY' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = max(md_record_written_timestamp)
FROM
    cons_retail.store_consultant_kpi_daily;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    cons_retail.store_consultant_kpi_daily
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
    'cons_customer.sp_store_consultant_kpi_daily' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
