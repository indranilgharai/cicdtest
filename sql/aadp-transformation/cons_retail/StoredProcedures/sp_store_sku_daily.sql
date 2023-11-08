/****** Object:  StoredProcedure [cons_retail].[sp_store_sku_daily]    Script Date: 1/24/2023 1:34:48 PM ******/
/****** Modified:  Modified StoredProcedure [cons_retail].[sp_store_sku_daily] 
Changes: Added new 365 days calculations in new columns
Modified Date: 5/01/2023 11:54:48 AM  Modified By: Harsha Varadhi ******/
/****** Modified: Updated batch_date logic to populate LY values correctly    Script Date: 05/15/2023 1:00:00 PM  Modified By: Patrick Lacerna ******/
/****** Modified: Updated Not null filter logic for LY_365 fields Script Date: 5/25/2023 1:00:00 PM Modified By: Harsha Varadhi ******/
/****** Modified: Updated merge logic to delete orphaned records    Script Date: 09/06/2023 3:00:00 PM  Modified By: Patrick Lacerna ******/
/****** Modified: Updated 
			1. logic to add unique combination of location_code and product_code for past 2 year to be driving factor and all calculation would be around it for each date.
			2. added 4 more columns for merge update logic     
Script Date: 09/14/2023 10:15:00 PM  Modified By: Rahul Trivedi ******/
/****** Modified: As per discussion  logic to add unique combination of location_code and product_code for past 2 year changed to past 17 months to be driving factor and all calculation would be around it for each date.
			Script Date: 09/15/2023 12:00:00 PM  Modified By: Rahul Trivedi ******/
/****** Modified: Added bundle_sku_line_no to inner join to ensure correct granularity    Script Date: 10/19/2023 13:00:00 PM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_retail].[sp_store_sku_daily] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN 
BEGIN 
TRY 
IF @reset = 0 
BEGIN 
DECLARE @batch_date [varchar](500)
DECLARE @start_date [varchar](500)
select 
    @start_date=	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(MONTH,-17,getdate())
	else dateadd(MONTH,-17,max(CAST(date_key as date))) end
    ,@batch_date=	case when ((DATEPART(WEEKDAY, getdate()) in (7) and DATEPART(HOUR, GETDATE())<=9) or max(cast(date_key as date)) is null)
	then cast('2012-01-01' as date)
	when max(CAST(date_key as date))>getdate() then dateadd(month,-4,getdate())
	else dateadd(month,-4,max(CAST(date_key as date))) end
from
	cons_retail.store_sku_daily;

IF OBJECT_ID('tempdb..#storeskudaily') IS NOT  NULL
BEGIN
    DROP TABLE #storeskudaily
END
create table #storeskudaily
with
(distribution=round_robin,
clustered index(storeskukey)
)
as 

WITH base_query as (
	select
		date_code,
		location_code,
		product_key,
		sum(sales_units) as units,
		sum(revenue_in_aud) as revenue_in_aud
	from
		(
			select DISTINCT 
				pr.orderid,
				retail_transaction_line_itemid,
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
				) date_code,
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
				prli.product_code as product_key,
				sales_units,
				 (
                                    CASE
                                        WHEN ISNULL(pr.Customer_ID, '') = '' THEN 0
                                        WHEN pr.Customer_ID like 'WI00%' THEN 0
                                        ELSE 1
                                    END
                                ) AS Linked_transactions,
				case
					when pr.currency_code = 'AUD' then prli.price
					else (
						cast(prli.price as float) / cast(exrate.ex_rate as FLOAT)
					)
				end as revenue_in_AUD
				
			FROM
			( select * from std.purchase_record  where 	cast(
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
				/*logic to fetch records based on specific product_type_sub_cat and category*/ 
				INNER JOIN (
					select
						*
					from
						std.product_x
					where
						product_type_sub_cat in ('Retail', 'Kit Item Only')
						and category not in ('Non Sale', 'Packaging Component', 'Voucher')
				) prd on prli.product_code = prd.description1 
				
		) innerQ
	group by
		date_code,
		location_code,
		product_key
)
/*Inserting data to target table ([cons_retail].[store_sku_daily])*/


,master_table
	as
	(
		select distinct b.date_code,
			/* Updated the LY date logic to retrieve D-364 */
			case when (((substring(cast(b.date_code as nvarchar),1,4)*1) %4) = 0
                    AND ((substring(cast(b.date_code as nvarchar),1,4)*1) %100) != 100
                    OR ((substring(cast(b.date_code as nvarchar),1,4)*1) %400 = 0)) 
                THEN
                    dateadd(day, -365, b.date_code)
                ELSE
                    dateadd(day, -364, b.date_code)
	        END AS date_code_prev
            , c.location_code
            , c.product_key
		from (select distinct location_code, product_key from base_query) c
		join
			(select distinct incr_date date_code
			from std.date_dim
			where incr_date<=(select max(date_code)
				from base_query) and incr_date>=@batch_date 
	        ) b
			on 1=1
		Left join base_query a
			on a.location_code=c.location_code and a.product_key = c.product_key
	)

,master_365 as 
(
select distinct b.date_code,
dateadd(year, -1, b.date_code) date_code_prev,
a.location_code,
a.product_key
 from base_query a
join
(select distinct incr_date date_code from std.date_dim where incr_date<=(select max(date_code) from base_query) and incr_date>=@batch_date 
) b
on 1=1
)

select
	cast(
		concat(
			format(mastr.date_code, 'yyyyMMdd'),
			mastr.location_code,
			mastr.product_key
		) as varchar(100)
	) as storeskukey,
	CAST(
		concat(
			format(mastr.date_code, 'yyyyMMdd'),
			mastr.location_code
		) AS varchar(250)
	) AS storekpikey,
	cast(mastr.date_code as date) as date_key,
	cast(mastr.location_code as varchar(10)) location_key,
	cast(mastr.product_key as varchar(50)) product_key,
	cast(a.units as int) units,
	cast(b.units_LY as int) units_LY,
    cast(c.units_LY as int) units_LY_365,
	cast(a.revenue_in_aud as float) revenue_in_aud,
cast(b.revenue_in_aud_LY as float) revenue_in_aud_LY,
cast(c.revenue_in_aud_LY as float) revenue_in_aud_LY_365,
	getdate() as md_record_written_timestamp,
	@pipelineid as md_record_written_pipeline_id,
	@jobid as md_transformation_job_id
from master_table mastr
Left join 
master_365 mastr_365
on mastr.location_code = mastr_365.location_code 
and CAST(mastr.date_code AS date) = CAST(mastr_365.date_code AS date) 
and mastr.product_key=mastr_365.product_key 
LEFT JOIN
	base_query a
	on mastr.date_code=a.date_code
	and mastr.location_code=a.location_code
	and mastr.product_key=a.product_key 
	LEFT JOIN (
		select
			date_code,
			location_code,
			product_key,
			units as units_LY,
			revenue_in_aud as revenue_in_aud_LY
		from
			base_query
	) b ON mastr.location_code = b.location_code
	and b.date_code = mastr.date_code_prev
	and mastr.product_key = b.product_key 
LEFT JOIN (
select
			date_code,
			location_code,
			product_key,
			units as units_LY,
			revenue_in_aud as revenue_in_aud_LY
		from
			base_query
	) c ON mastr_365.location_code = c.location_code
	and c.date_code = mastr_365.date_code_prev
	and mastr_365.product_key = c.product_key

	where (a.location_code is not null or b.location_code is not null or c.location_code is not null
	or a.product_key is not null or b.product_key is not null or c.product_key is not null
	or a.date_code is not null or b.date_code is not null or c.date_code is not null)
	and mastr.date_code >= @batch_date
	   	  
MERGE
INTO cons_retail.store_sku_daily as TargetTbl
USING #storeskudaily as SourceTbl
ON  SourceTbl.storeskukey= TargetTbl.storeskukey
WHEN MATCHED 
THEN UPDATE 
SET
TargetTbl.[storeskukey]	=SourceTbl.[storeskukey]	,
TargetTbl.[storekpikey]	=SourceTbl.[storekpikey]	,
TargetTbl.[location_key]=SourceTbl.[location_key]	,
TargetTbl.[product_key]=SourceTbl.[product_key]		,
TargetTbl.[date_key]=SourceTbl.[date_key]		,
TargetTbl.[revenue_in_aud]=SourceTbl.[revenue_in_aud]	,
TargetTbl.[units]=SourceTbl.[units]			,
TargetTbl.[md_record_written_timestamp]=SourceTbl.[md_record_written_timestamp]	,
TargetTbl.[md_record_written_pipeline_id]= SourceTbl.[md_record_written_pipeline_id]	,
TargetTbl.[md_transformation_job_id]=SourceTbl.[md_transformation_job_id],	
TargetTbl.[units_LY] = SourceTbl.[units_LY], 
TargetTbl.[units_LY_365] = SourceTbl.[units_LY_365], 
TargetTbl.[revenue_in_aud_LY] = SourceTbl.[revenue_in_aud_LY],
TargetTbl.[revenue_in_aud_LY_365] = SourceTbl.[revenue_in_aud_LY_365]	
WHEN NOT MATCHED BY TARGET
THEN INSERT 
([storeskukey],
[storekpikey],
[date_key],
[location_key],[product_key],
[units],
[units_LY],
[units_LY_365],
[revenue_in_aud],
[revenue_in_aud_LY],
[revenue_in_aud_LY_365],
[md_record_written_timestamp],
[md_record_written_pipeline_id],
[md_transformation_job_id])
VALUES 
(SourceTbl.[storeskukey], 
SourceTbl.[storekpikey],
SourceTbl.[date_key], 
SourceTbl.[location_key], 
SourceTbl.[product_key],
SourceTbl.[units], 
SourceTbl.[units_LY], 
SourceTbl.[units_LY_365], 
SourceTbl.[revenue_in_aud], 
SourceTbl.[revenue_in_aud_LY],
SourceTbl.[revenue_in_aud_LY_365], 
SourceTbl.[md_record_written_timestamp], SourceTbl.[md_record_written_pipeline_id], SourceTbl.[md_transformation_job_id])

/* Added logic to delete rows where streaming location_code has been remapped by EOD cegid_online_seller. 
Date condition included to ensure data prior to batch_date doesn't get deleted */
WHEN NOT MATCHED BY SOURCE and TargetTbl.[date_key] >= @batch_date
THEN DELETE;

UPDATE STATISTICS cons_retail.store_sku_daily;

/*BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT*/
DECLARE @label VARCHAR(500)
SET
	@label = 'AADPRETKPISTOSKUDLY' EXEC meta_ctl.sp_row_count @jobid,
	@step_number,
	@label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
	@newrec = max(md_record_written_timestamp)
FROM
	cons_retail.store_sku_daily;

SELECT
	@onlydate = CAST(@newrec AS DATE);

DELETE FROM
	cons_retail.store_sku_daily
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
	'cons_customer.sp_store_sku_daily' AS ErrorProcedure,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() AS Updated_date
END CATCH
END
