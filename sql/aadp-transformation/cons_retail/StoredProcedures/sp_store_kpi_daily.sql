/****** Object:  StoredProcedure [cons_retail].[sp_store_kpi_daily]    Script Date: 1/24/2023 1:33:30 PM ******/
/****** Modified: Added logic to remove customergifts when products are selected        Script Date: 03/08/2023 3:39:00 PM ******/
/****** Modified:  Modified StoredProcedure [cons_retail].[sp_store_kpi_daily] 
Changes: Added new 365 days calculations in new columns
Modified Date: 5/01/2023 11:54:48 AM  Modified By: Harsha Varadhi ******/
/****** Modified: Updated batch_date logic to populate LY values correctly    Script Date: 05/15/2023 1:00:00 PM  Modified By: Patrick Lacerna ******/
/****** Modified: Updated result table filtering logic to capture LY revenue in the year after store closure    Script Date: 10/16/2023 3:00:00 PM  Modified By: Cissy Shu ******/
/****** Modified: Added bundle_sku_line_no to inner join to ensure correct granularity    Script Date: 10/19/2023 13:00:00 PM  Modified By: Patrick Lacerna ******/
/****** Modified: Updated result table filtering logic to remove future date and rows with all Null values for performance optimisation    Script Date: 23/16/2023 11:00:00 AM  Modified By: Cissy Shu ******/
/****** Modified: Updated result table add future date back in as per user requirement    Script Date: 31/10/2023 11:32:00 AM  Modified By: Cissy Shu ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_store_kpi_daily] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY IF @reset = 0 BEGIN


DECLARE @batch_date [varchar](500)
DECLARE @start_date [varchar](500)
select 
    @start_date=	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(day,-366,getdate())
	else dateadd(day,-366,max(CAST(date_key as date))) end
    ,@batch_date=	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(month,-4,getdate())
	else dateadd(month,-4,max(CAST(date_key as date))) end
from
	cons_retail.store_kpi_daily;

IF OBJECT_ID('tempdb..#storekpidaily') IS NOT  NULL
BEGIN
    DROP TABLE #storekpidaily
END
create table #storekpidaily
with
(distribution=round_robin,
clustered index(storekpikey)
)
as 

/***storekpi cte - to get required revenue details,transactions,budget,target,employeecode,location_code etc ***/

WITH targbudg as (select 
FORMAT(CAST(main.sbs_no AS INT), '00', 'en-US') + FORMAT(CAST(main.store_no AS INT), '000', 'en-US') location_code,
cast(dateval as date) dateval,
CAST(daily_target AS FLOAT) / CAST(exrate.ex_rate AS FLOAT) target_aud,
        CAST(sales_budget AS FLOAT) / CAST(exrate.ex_rate AS FLOAT) budget_aud
FROM(
select coalesce(targ.sbs_no,budg.sbs_no) sbs_no,coalesce(targ.store_no,budg.store_no) store_no,
coalesce(targ.yyyymmdd,budg.budget_yyyymmdd) dateval,
daily_target,sales_budget from 
(select * from std.dwh_store_sales_target_daily where isdate(yyyymmdd) = 1 )targ
full outer join std.dwh_store_budget_daily budg
on targ.sbs_no=budg.sbs_no
and targ.store_no=budg.store_no
and budg.budget_yyyymmdd=targ.yyyymmdd
)main
LEFT JOIN 
(SELECT * FROM (SELECT sbs_no,ex_rate,year,month_no,ROW_NUMBER() OVER(PARTITION BY sbs_no ORDER BY year DESC,month_no DESC) rwno
FROM [std].[exchange_rate_x] ) a WHERE rwno = 1 ) exrate 
ON CAST(main.sbs_no AS INT) = CAST(exrate.sbs_no AS INT)
),

 storekpi as (
	/**fetching the required attributes**/
    SELECT
      coalesce(outerq2.location_code,targbudg.location_code)  location_code,
        coalesce(outerq2.create_date_purchase,dateval) create_date_purchase,
        revenue_in_aud,
        target_aud,
        budget_aud,
          transactions,
        multi_unit_transactions,
        new_customer_transactions,
        linked_transactions,
        multi_category_transactions,
        units,
        traffic,
        shopfront_traffic_open,
        shopfront_traffic_closed,
        shopfront_conversion,
        bounces60,
		bounces120,
        in_store_secs,
        skincare_revenue_aud,
        bodycare_revenue_aud,
        fragrance_revenue_aud,
        haircare_revenue_aud,
        home_revenue_aud,
        kits_revenue_aud
    FROM
        (
            SELECT
                location_code,
                create_date_purchase,
                SUM(revenue_in_aud) revenue_in_aud,
                COUNT(distinct ORDERid) transactions,
                /**logic for multi_unit_transactions**/
				SUM(
                    CASE
                        WHEN units > 1 THEN 1
                        ELSE 0
                    END
                ) multi_unit_transactions,
				
				/**logic for New customer**/
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
				/**aggregating category based revenue fields and units**/
                SUM(units) units,
                SUM(skincare_revenue_aud) AS skincare_revenue_aud,
                SUM(bodycare_revenue_aud) AS bodycare_revenue_aud,
                SUM(fragrance_revenue_aud) AS fragrance_revenue_aud,
                SUM(haircare_revenue_aud) AS haircare_revenue_aud,
                SUM(home_revenue_aud) AS home_revenue_aud,
                SUM(kits_revenue_aud) AS kits_revenue_aud
            FROM
                (
				/*fetching required columns at location_code and create_date_purchase level*/
                    SELECT
                        ORDERid,
                        location_code,
                        create_date_purchase,
                        SUM(revenue_in_aud) revenue_in_aud,
                        SUM(sales_units) AS units,
                        new_customer,
                        linked_transactions,
                        COUNT(distinct category) category,
						/**aggregating category based revenue fields**/
                        SUM(skincare_revenue_aud) AS skincare_revenue_aud,
                        SUM(bodycare_revenue_aud) AS bodycare_revenue_aud,
                        SUM(fragrance_revenue_aud) AS fragrance_revenue_aud,
                        SUM(haircare_revenue_aud) AS haircare_revenue_aud,
                        SUM(home_revenue_aud) AS home_revenue_aud,
                        SUM(kits_revenue_aud) AS kits_revenue_aud
                    FROM
                        (
                            SELECT
                                DISTINCT 
								/*logic to find location_code*/
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
                                    end as date
                                ) create_date_purchase,
                                retail_transaction_line_itemid,
                                bundle_sku_line_no,
								/*logic to find revenue_in_AUD*/
                                case
                                    when pr.currency_code = 'AUD' then prli.price
                                    else (
                                        cast(prli.price as float) / cast(exrate.ex_rate as FLOAT)
                                    )
                                end as revenue_in_AUD,
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
								
								/************************category based revenue************************/
								/*Revenue logic of category: SKIN CARE*/
                                CASE
                                    WHEN prd.category = 'SKIN CARE' THEN (
                                        CASE
                                            WHEN pr.currency_code = 'AUD' THEN prli.price
                                            ELSE (
                                                CAST(prli.price AS FLOAT) / CAST(exrate.ex_rate AS FLOAT)
                                            )
                                        END
                                    )
                                END AS skincare_revenue_aud,
								/*Revenue logic of category: BODY CARE*/
                                CASE
                                    WHEN prd.category = 'BODY CARE' THEN (
                                        CASE
                                            WHEN pr.currency_code = 'AUD' THEN prli.price
                                            ELSE (
                                                CAST(prli.price AS FLOAT) / CAST(exrate.ex_rate AS FLOAT)
                                            )
                                        END
                                    )
                                END AS bodycare_revenue_aud,
								/*Revenue logic of category: FRAGRANCE*/
                                CASE
                                    WHEN prd.category = 'FRAGRANCE' THEN (
                                        CASE
                                            WHEN pr.currency_code = 'AUD' THEN prli.price
                                            ELSE (
                                                CAST(prli.price AS FLOAT) / CAST(exrate.ex_rate AS FLOAT)
                                            )
                                        END
                                    )
                                END AS fragrance_revenue_aud,
								/*Revenue logic of category: HAIR/HAIR CARE*/
                                CASE
                                    WHEN prd.category in ('HAIR', 'HAIR CARE') THEN (
                                        CASE
                                            WHEN pr.currency_code = 'AUD' THEN prli.price
                                            ELSE (
                                                CAST(prli.price AS FLOAT) / CAST(exrate.ex_rate AS FLOAT)
                                            )
                                        END
                                    )
                                END AS haircare_revenue_aud,
								/*Revenue logic of category: HOME*/
                                CASE
                                    WHEN prd.category = 'HOME' THEN (
                                        CASE
                                            WHEN pr.currency_code = 'AUD' THEN prli.price
                                            ELSE (
                                                CAST(prli.price AS FLOAT) / CAST(exrate.ex_rate AS FLOAT)
                                            )
                                        END
                                    )
                                END AS home_revenue_aud,
								/*Revenue logic of category: KITS*/
                                CASE
                                    WHEN prd.category = 'KITS' THEN (
                                        CASE
                                            WHEN pr.currency_code = 'AUD' THEN prli.price
                                            ELSE (
                                                CAST(prli.price AS FLOAT) / CAST(exrate.ex_rate AS FLOAT)
                                            )
                                        END
                                    )
                                END AS kits_revenue_aud,
                                exrate.ex_rate,
                                pr.ORDERid,
								/*Logic to find new customer*/
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
                                (select * from std.purchase_record where cast(
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
				) >=@start_date )pr
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
                                                    ORDER BY
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
										/*logic to find price*/
										case
                                            when source_system = 'HYBRIS'
                                            and return_flag = 'Y' then (revenue_tax_exc_local - return_value)
                                            when source_system = 'HYBRIS'
                                            and cancelled_flag = 'Y' then (revenue_tax_exc_local - cancellation_value)
                                            when source_system = 'HYBRIS'
                                            and (
                                                return_flag = 'Y'
                                                and cancelled_flag = 'Y'
                                            ) then (
                                                revenue_tax_exc_local - return_value - cancellation_value
                                            )
                                            else revenue_tax_exc_local
                                        end AS price,
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
                                        and description1 not in ('CUSTOMERGIFT') /*to remover customergifts*/
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

		FULL OUTER JOIN
		(select * from targbudg where dateval>=@batch_date) targbudg
		on targbudg.location_code=outerq2.location_code
		and targbudg.dateval=outerq2.create_date_purchase


        LEFT JOIN (
            SELECT
                FORMAT(CAST(sbs_no AS INT), '00', 'en-US') + FORMAT(CAST(store_no AS INT), '000', 'en-US') locationcode,
                CONVERT(date, CAST(traffic_date AS VARCHAR)) traffic_date,
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
                CONVERT(date, CAST(traffic_date AS VARCHAR))
   ) traffic ON coalesce(outerq2.location_code,targbudg.location_code) = traffic.locationcode
        and coalesce(outerq2.create_date_purchase,targbudg.dateval) = traffic_date
	
)
,master_table as 
(
Select distinct b.create_date_purchase,
/* Updated the LY date logic to retrieve D-364 */
case when (((substring(cast(b.create_date_purchase as nvarchar),1,4)*1) %4) = 0 
AND ((substring(cast(b.create_date_purchase as nvarchar),1,4)*1) %100) != 100
OR ((substring(cast(b.create_date_purchase as nvarchar),1,4)*1) %400 = 0)) 
THEN
	dateadd(day, -365, b.create_date_purchase)
  ELSE
	dateadd(day, -364, b.create_date_purchase)
  END AS create_date_purchase_LY
,a.location_code 
from storekpi a
join
(select distinct incr_date create_date_purchase from std.date_dim where incr_date<=(select max(create_date_purchase) from storekpi) and incr_date>=@batch_date 
) b
on 1=1
),

master_365 as 
(
select distinct b.create_date_purchase,dateadd(year, -1, b.create_date_purchase) create_date_purchase_LY,a.location_code from storekpi a
join
(select distinct incr_date create_date_purchase from std.date_dim where incr_date<=(select max(create_date_purchase) from storekpi) and incr_date>=@batch_date 
) b
on 1=1
)


SELECT
    CAST(
        concat(
            format(mastr.create_date_purchase, 'yyyyMMdd'),
            mastr.location_code
        ) AS VARCHAR(250)
    ) AS storekpikey,
    CAST(mastr.location_code AS VARCHAR(10)) location_key,
    CAST(mastr.create_date_purchase AS date) date_key,
    CAST(mastr.create_date_purchase_LY AS date) date_key_LY,
    CAST(mastr_365.create_date_purchase_LY AS date) date_key_LY_365,
    CAST(revenue_in_aud AS FLOAT) revenue_in_aud,
    CAST(b.revenue_in_aud_LY AS FLOAT) revenue_in_aud_LY,
    CAST(c.revenue_in_aud_LY AS FLOAT) revenue_in_aud_LY_365,
    CAST(target_aud AS FLOAT) target_aud,
    CAST(b.target_aud_LY AS FLOAT) target_aud_LY,
    CAST(c.target_aud_LY AS FLOAT) target_aud_LY_365,
    CAST(budget_aud AS FLOAT) budget_aud,
    CAST(b.budget_aud_LY AS FLOAT) budget_aud_LY,
    CAST(c.budget_aud_LY AS FLOAT) budget_aud_LY_365,
    CAST(transactions AS INT) transactions,
    CAST(b.transactions_LY AS INT) transactions_LY,
    CAST(c.transactions_LY AS INT) transactions_LY_365,
    CAST(multi_unit_transactions AS INT) multi_unit_transactions,
    CAST(b.multi_unit_transactions_LY AS INT) multi_unit_transactions_LY,
    CAST(c.multi_unit_transactions_LY AS INT) multi_unit_transactions_LY_365,
    CAST(new_customer_transactions AS INT) new_customer_transactions,
    CAST(b.new_customer_transactions_LY AS INT) new_customer_transactions_LY,
    CAST(c.new_customer_transactions_LY AS INT) new_customer_transactions_LY_365,
    CAST(linked_transactions AS INT) linked_transactions,
    CAST(b.linked_transactions_LY AS INT) linked_transactions_LY,
    CAST(c.linked_transactions_LY AS INT) linked_transactions_LY_365,
    CAST(multi_category_transactions AS INT) multi_category_transactions,
    CAST(b.multi_category_transactions_LY AS INT) multi_category_transactions_LY,
    CAST(c.multi_category_transactions_LY AS INT) multi_category_transactions_LY_365,
    CAST(units AS INT) units,
    CAST(b.units_LY AS INT) units_LY,
    CAST(c.units_LY AS INT) units_LY_365,
    CAST(traffic AS INT) traffic,
    CAST(b.traffic_LY AS INT) traffic_LY,
    CAST(c.traffic_LY AS INT) traffic_LY_365,
    CAST(shopfront_traffic_open AS INT) shopfront_traffic_open,
    CAST(b.shopfront_traffic_open_LY AS INT) shopfront_traffic_open_LY,
    CAST(c.shopfront_traffic_open_LY AS INT) shopfront_traffic_open_LY_365,
    CAST(shopfront_traffic_closed AS INT) shopfront_traffic_closed,
    CAST(b.shopfront_traffic_closed_LY AS INT) shopfront_traffic_closed_LY,
    CAST(c.shopfront_traffic_closed_LY AS INT) shopfront_traffic_closed_LY_365,
    CAST(bounces60 AS INT) bounces60,
    CAST(b.bounces60_LY AS INT) bounces60_LY,
    CAST(c.bounces60_LY AS INT) bounces60_LY_365,
    CAST(bounces120 AS INT) bounces120,
    CAST(b.bounces120_LY AS INT) bounces120_LY,
    CAST(c.bounces120_LY AS INT) bounces120_LY_365,
    CAST(in_store_secs AS INT) in_store_secs,
    CAST(b.in_store_secs_LY AS INT) in_store_secs_LY,
    CAST(c.in_store_secs_LY AS INT) in_store_secs_LY_365,
    CAST(skincare_revenue_aud AS FLOAT) skincare_revenue_aud,
    CAST(b.skincare_revenue_aud_LY AS FLOAT) skincare_revenue_aud_LY,
    CAST(c.skincare_revenue_aud_LY AS FLOAT) skincare_revenue_aud_LY_365,
    CAST(bodycare_revenue_aud AS FLOAT) bodycare_revenue_aud,
    CAST(b.bodycare_revenue_aud_LY AS FLOAT) bodycare_revenue_aud_LY,
    CAST(c.bodycare_revenue_aud_LY AS FLOAT) bodycare_revenue_aud_LY_365,
    CAST(fragrance_revenue_aud AS FLOAT) fragrance_revenue_aud,
    CAST(b.fragrance_revenue_aud_LY AS FLOAT) fragrance_revenue_aud_LY,
    CAST(c.fragrance_revenue_aud_LY AS FLOAT) fragrance_revenue_aud_LY_365,
    CAST(haircare_revenue_aud AS FLOAT) haircare_revenue_aud,
    CAST(b.haircare_revenue_aud_LY AS FLOAT) haircare_revenue_aud_LY,
    CAST(c.haircare_revenue_aud_LY AS FLOAT) haircare_revenue_aud_LY_365,
    CAST(home_revenue_aud AS FLOAT) home_revenue_aud,
    CAST(b.home_revenue_aud_LY AS FLOAT) home_revenue_aud_LY,
    CAST(c.home_revenue_aud_LY AS FLOAT) home_revenue_aud_LY_365,
    CAST(kits_revenue_aud AS FLOAT) kits_revenue_aud,
    CAST(b.kits_revenue_aud_LY AS FLOAT) kits_revenue_aud_LY,
    CAST(c.kits_revenue_aud_LY AS FLOAT) kits_revenue_aud_LY_365,
    getDate() AS md_record_written_timestamp,
    @pipelineid AS md_record_written_pipeline_id,
    @jobid AS md_transformation_job_id
FROM
master_table mastr
LEFT JOIN
master_365 mastr_365 
on mastr.location_code = mastr_365.location_code 
and CAST(mastr.create_date_purchase AS date) = CAST(mastr_365.create_date_purchase AS date)
LEFT JOIN
    storekpi a
    on mastr.create_date_purchase = a.create_date_purchase
    and mastr.location_code = a.location_code
    LEFT JOIN (
        SELECT
            location_code,
            create_date_purchase create_date_purchase_LY,
            revenue_in_aud revenue_in_aud_LY,
            target_aud target_aud_LY,
            budget_aud budget_aud_LY,
            transactions transactions_LY,
            multi_unit_transactions multi_unit_transactions_LY,
            new_customer_transactions new_customer_transactions_LY,
            linked_transactions linked_transactions_LY,
            multi_category_transactions multi_category_transactions_LY,
            units units_LY,
            traffic traffic_LY,
            shopfront_traffic_open shopfront_traffic_open_LY,
            shopfront_traffic_closed shopfront_traffic_closed_LY,
            shopfront_conversion shopfront_conversion_LY,
            bounces60 bounces60_LY,
            bounces120 bounces120_LY,
            in_store_secs in_store_secs_LY,
            skincare_revenue_aud skincare_revenue_aud_LY,
            bodycare_revenue_aud bodycare_revenue_aud_LY,
            fragrance_revenue_aud fragrance_revenue_aud_LY,
            haircare_revenue_aud haircare_revenue_aud_LY,
            home_revenue_aud home_revenue_aud_LY,
            kits_revenue_aud kits_revenue_aud_LY
        FROM
            storekpi
    ) b ON mastr.location_code = b.location_code
    and b.create_date_purchase_LY = mastr.create_date_purchase_LY
    LEFT JOIN (
        SELECT
            location_code,
            create_date_purchase create_date_purchase_LY,
            revenue_in_aud revenue_in_aud_LY,
            target_aud target_aud_LY,
            budget_aud budget_aud_LY,
            transactions transactions_LY,
            multi_unit_transactions multi_unit_transactions_LY,
            new_customer_transactions new_customer_transactions_LY,
            linked_transactions linked_transactions_LY,
            multi_category_transactions multi_category_transactions_LY,
            units units_LY,
            traffic traffic_LY,
            shopfront_traffic_open shopfront_traffic_open_LY,
            shopfront_traffic_closed shopfront_traffic_closed_LY,
            shopfront_conversion shopfront_conversion_LY,
            bounces60 bounces60_LY,
            bounces120 bounces120_LY,
            in_store_secs in_store_secs_LY,
            skincare_revenue_aud skincare_revenue_aud_LY,
            bodycare_revenue_aud bodycare_revenue_aud_LY,
            fragrance_revenue_aud fragrance_revenue_aud_LY,
            haircare_revenue_aud haircare_revenue_aud_LY,
            home_revenue_aud home_revenue_aud_LY,
            kits_revenue_aud kits_revenue_aud_LY
        FROM
            storekpi
    ) c ON mastr.location_code = c.location_code
    and c.create_date_purchase_LY = mastr_365.create_date_purchase_LY

    --- changing a to master_table as the old filter 'a.create_date_purchase is not null OR a.location_code is not null 
    -- drops date-location combination for the year after a store is closed
    WHERE
    (
    mastr.create_date_purchase IS NOT NULL 
    or mastr.location_code IS NOT NULL
    )
    -- Exclude rows where all values are Null
	AND (
    revenue_in_aud IS NOT NULL
	OR b.revenue_in_aud_LY IS NOT NULL
	OR c.revenue_in_aud_LY IS NOT NULL
	OR target_aud IS NOT NULL
	OR b.target_aud_LY IS NOT NULL
	OR c.target_aud_LY IS NOT NULL
	OR budget_aud IS NOT NULL
	OR b.budget_aud_LY IS NOT NULL
	OR c.budget_aud_LY IS NOT NULL
	OR transactions IS NOT NULL
	OR b.transactions_LY IS NOT NULL
	OR c.transactions_LY IS NOT NULL
	OR multi_unit_transactions IS NOT NULL
	OR b.multi_unit_transactions_LY IS NOT NULL
	OR c.multi_unit_transactions_LY IS NOT NULL
	OR new_customer_transactions IS NOT NULL
	OR b.new_customer_transactions_LY IS NOT NULL
	OR c.new_customer_transactions_LY IS NOT NULL
	OR linked_transactions IS NOT NULL
	OR b.linked_transactions_LY IS NOT NULL
	OR c.linked_transactions_LY IS NOT NULL
	OR multi_category_transactions IS NOT NULL
	OR b.multi_category_transactions_LY IS NOT NULL
	OR c.multi_category_transactions_LY IS NOT NULL
	OR units IS NOT NULL
	OR b.units_LY IS NOT NULL
	OR c.units_LY IS NOT NULL
	OR traffic IS NOT NULL
	OR b.traffic_LY IS NOT NULL
	OR c.traffic_LY IS NOT NULL
	OR shopfront_traffic_open IS NOT NULL
	OR b.shopfront_traffic_open_LY IS NOT NULL
	OR c.shopfront_traffic_open_LY IS NOT NULL
	OR shopfront_traffic_closed IS NOT NULL
	OR b.shopfront_traffic_closed_LY IS NOT NULL
	OR c.shopfront_traffic_closed_LY IS NOT NULL
	OR bounces60 IS NOT NULL
	OR b.bounces60_LY IS NOT NULL
	OR c.bounces60_LY IS NOT NULL
	OR bounces120 IS NOT NULL
	OR b.bounces120_LY IS NOT NULL
	OR c.bounces120_LY IS NOT NULL
	OR in_store_secs IS NOT NULL
	OR b.in_store_secs_LY IS NOT NULL
	OR c.in_store_secs_LY IS NOT NULL
    )
    AND mastr.create_date_purchase >= @batch_date
	OPTION (LABEL = 'AADPRETKPILOCATDAILY');



MERGE
INTO cons_retail.store_kpi_daily as TargetTbl
USING #storekpidaily as SourceTbl
ON  SourceTbl.storekpikey= TargetTbl.storekpikey
WHEN MATCHED 
THEN UPDATE SET
TargetTbl.[storekpikey]	=SourceTbl.[storekpikey]	,
TargetTbl.[location_key]	=SourceTbl.[location_key]	,
TargetTbl.[date_key]	=SourceTbl.[date_key]	,
TargetTbl.[revenue_in_aud]	=SourceTbl.[revenue_in_aud]	,
TargetTbl.[target_aud]	=SourceTbl.[target_aud]	,
TargetTbl.[budget_aud]	=SourceTbl.[budget_aud]	,
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
TargetTbl.[skincare_revenue_aud]	=SourceTbl.[skincare_revenue_aud]	,
TargetTbl.[bodycare_revenue_aud]	=SourceTbl.[bodycare_revenue_aud]	,
TargetTbl.[fragrance_revenue_aud]	=SourceTbl.[fragrance_revenue_aud]	,
TargetTbl.[haircare_revenue_aud]	=SourceTbl.[haircare_revenue_aud]	,
TargetTbl.[home_revenue_aud]	=SourceTbl.[home_revenue_aud]	,
TargetTbl.[kits_revenue_aud]	=SourceTbl.[kits_revenue_aud]	,
TargetTbl.[md_record_written_timestamp]	=SourceTbl.[md_record_written_timestamp]	,
TargetTbl.[md_record_written_pipeline_id]	= SourceTbl.[md_record_written_pipeline_id]	,
TargetTbl.[md_transformation_job_id]	=SourceTbl.[md_transformation_job_id]	
WHEN NOT MATCHED BY TARGET
THEN 
INSERT 
([storekpikey], 
[location_key], 
[date_key], 
[date_key_LY], 
[date_key_LY_365], 
[revenue_in_aud], 
[revenue_in_aud_LY],
[revenue_in_aud_LY_365], 
[target_aud], 
[target_aud_LY],
[target_aud_LY_365], 
[budget_aud], 
[budget_aud_LY], 
[budget_aud_LY_365], 
[transactions], 
[transactions_LY],
[transactions_LY_365], 
[multi_unit_transactions], 
[multi_unit_transactions_LY], 
[multi_unit_transactions_LY_365], 
[new_customer_transactions], 
[new_customer_transactions_LY], 
[new_customer_transactions_LY_365], 
[linked_transactions], 
[linked_transactions_LY], 
[linked_transactions_LY_365], 
[multi_category_transactions], 
[multi_category_transactions_LY], 
[multi_category_transactions_LY_365], 
[units], 
[units_LY], 
[units_LY_365], 
[traffic], 
[traffic_LY], 
[traffic_LY_365], 
[shopfront_traffic_open], 
[shopfront_traffic_open_LY], 
[shopfront_traffic_open_LY_365], 
[shopfront_traffic_closed], 
[shopfront_traffic_closed_LY], 
[shopfront_traffic_closed_LY_365], 
[bounces60], 
[bounces60_LY], 
[bounces60_LY_365], 
[bounces120], 
[bounces120_LY], 
[bounces120_LY_365], 
[in_store_secs], 
[in_store_secs_LY],
[in_store_secs_LY_365], 
[skincare_revenue_aud], 
[skincare_revenue_aud_LY], 
[skincare_revenue_aud_LY_365], 
[bodycare_revenue_aud], 
[bodycare_revenue_aud_LY], 
[bodycare_revenue_aud_LY_365], 
[fragrance_revenue_aud], 
[fragrance_revenue_aud_LY], 
[fragrance_revenue_aud_LY_365], 
[haircare_revenue_aud], 
[haircare_revenue_aud_LY], 
[haircare_revenue_aud_LY_365], 
[home_revenue_aud], 
[home_revenue_aud_LY], 
[home_revenue_aud_LY_365], 
[kits_revenue_aud], 
[kits_revenue_aud_LY], 
[kits_revenue_aud_LY_365], 
[md_record_written_timestamp],
[md_record_written_pipeline_id], 
[md_transformation_job_id])
VALUES 
(SourceTbl.[storekpikey], 
SourceTbl.[location_key], 
SourceTbl.[date_key], 
SourceTbl.[date_key_LY], 
SourceTbl.[date_key_LY_365], 
SourceTbl.[revenue_in_aud], 
SourceTbl.[revenue_in_aud_LY], 
SourceTbl.[revenue_in_aud_LY_365], 
SourceTbl.[target_aud], 
SourceTbl.[target_aud_LY], 
SourceTbl.[target_aud_LY_365], 
SourceTbl.[budget_aud], 
SourceTbl.[budget_aud_LY], 
SourceTbl.[budget_aud_LY_365], 
SourceTbl.[transactions], 
SourceTbl.[transactions_LY], 
SourceTbl.[transactions_LY_365], 
SourceTbl.[multi_unit_transactions], 
SourceTbl.[multi_unit_transactions_LY], 
SourceTbl.[multi_unit_transactions_LY_365], 
SourceTbl.[new_customer_transactions], 
SourceTbl.[new_customer_transactions_LY], 
SourceTbl.[new_customer_transactions_LY_365], 
SourceTbl.[linked_transactions], 
SourceTbl.[linked_transactions_LY], 
SourceTbl.[linked_transactions_LY_365], 
SourceTbl.[multi_category_transactions], 
SourceTbl.[multi_category_transactions_LY], 
SourceTbl.[multi_category_transactions_LY_365], 
SourceTbl.[units], SourceTbl.[units_LY], SourceTbl.[units_LY_365], 
SourceTbl.[traffic], SourceTbl.[traffic_LY], SourceTbl.[traffic_LY_365],
SourceTbl.[shopfront_traffic_open], SourceTbl.[shopfront_traffic_open_LY], SourceTbl.[shopfront_traffic_open_LY_365], 
SourceTbl.[shopfront_traffic_closed], SourceTbl.[shopfront_traffic_closed_LY], SourceTbl.[shopfront_traffic_closed_LY_365], 
SourceTbl.[bounces60], SourceTbl.[bounces60_LY], SourceTbl.[bounces60_LY_365], 
SourceTbl.[bounces120], SourceTbl.[bounces120_LY], SourceTbl.[bounces120_LY_365], 
SourceTbl.[in_store_secs], SourceTbl.[in_store_secs_LY], SourceTbl.[in_store_secs_LY_365], 
SourceTbl.[skincare_revenue_aud], SourceTbl.[skincare_revenue_aud_LY], SourceTbl.[skincare_revenue_aud_LY_365], 
SourceTbl.[bodycare_revenue_aud], SourceTbl.[bodycare_revenue_aud_LY], SourceTbl.[bodycare_revenue_aud_LY_365], 
SourceTbl.[fragrance_revenue_aud], SourceTbl.[fragrance_revenue_aud_LY], SourceTbl.[fragrance_revenue_aud_LY_365], 
SourceTbl.[haircare_revenue_aud], SourceTbl.[haircare_revenue_aud_LY], SourceTbl.[haircare_revenue_aud_LY_365], 
SourceTbl.[home_revenue_aud], SourceTbl.[home_revenue_aud_LY], SourceTbl.[home_revenue_aud_LY_365], 
SourceTbl.[kits_revenue_aud], SourceTbl.[kits_revenue_aud_LY], SourceTbl.[kits_revenue_aud_LY_365], 
SourceTbl.[md_record_written_timestamp], SourceTbl.[md_record_written_pipeline_id], SourceTbl.[md_transformation_job_id])	;



/*BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT*/
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPRETKPILOCATDAILY' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = max(md_record_written_timestamp)
FROM
    cons_retail.store_kpi_daily;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    cons_retail.store_kpi_daily
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
    'cons_customer.sp_store_kpi_daily' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
