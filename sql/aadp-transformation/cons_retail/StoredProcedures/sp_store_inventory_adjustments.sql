SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*****Stored procedure to generate revenue,location,product,adjustment_reason and unit details*****/
CREATE PROC [cons_retail].[sp_store_inventory_adjustments] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY 
    IF @reset = 0 
    BEGIN 
    DECLARE @max_ingestion_date_cons [varchar](500)
    SELECT
        @max_ingestion_date_cons = MAX(CAST([md_record_written_timestamp] AS date))
    FROM
        [cons_retail].[store_inventory_adjustments];
    
	
    TRUNCATE TABLE [cons_retail].[store_inventory_adjustments];
    
    /***cte_main - to get required revenue and sales details between stocktake_date and last_stocktake_date ***/
    WITH cte_main AS (
      /*fetching required attributes between current and last stock take date */
		SELECT location_code,product_code,
		
		SUM(revenue_since_last_stocktake) as revenue_since_last_stocktake,
		SUM(units_since_last_stocktake) as units_since_last_stocktake
		
		
		FROM 
			(SELECT
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
				li.product_code,
				revenue_tax_exc_AUD AS revenue_since_last_stocktake,
				li.sales_units AS units_since_last_stocktake,
				
				
				sc.stocktake_name,
				sc.stocktake_qtr,
				sc.stocktake_year,
				sc.stocktake_date,
			
			
				sc.last_stocktake_name,
				sc.last_stocktake_qtr,
				sc.last_stocktake_year,
				sc.last_stocktake_date
			FROM
				[std].[dimstocktake_schedule] sc
			
				JOIN [std].[purchase_record] pr on sc.locationkey = pr.location_code
				JOIN (select * from [std].[purchase_record_line_item]  WHERE
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
				
					)as li ON pr.orderid = li.orderid
				
				JOIN (
						SELECT
							*
						FROM
							std.product_x
						WHERE
							product_type_sub_cat in ('Retail', 'Kit Item Only')
							and category not in ('Non Sale', 'Packaging Component', 'Voucher')
					) prd on li.product_code=prd.description1
			WHERE
				pr.location_code is not null and li.product_code is not null 
			
		
			) main
		
			where CAST(create_date_purchase AS date) BETWEEN DATEADD(DAY,1,CAST(last_stocktake_date AS date)) AND CAST(stocktake_date AS date)
			--where CAST(create_date_purchase AS date) BETWEEN CAST('2022-02-09' AS date) AND CAST('2022-07-13' AS date)
			group by location_code,
			product_code
		
		
    )
    /*Inserting data to target table ([cons_retail].[store_inventory_adjustments])*/
    INSERT INTO
      [cons_retail].[store_inventory_adjustments]
    /*fetching the required attributes*/
    
	SELECT
		main.datekey,
		main.locationKey,
		main.productKey,
		main.location_productKey,
        main.adjustment_code,
        main.adjustment_reason,
        sum(main.adjustment_units) adjustment_units,
        main.item_cost_aud,
		main.item_rrp_aud,
		getDate() AS md_record_written_timestamp,
	    @pipelineid AS md_record_written_pipeline_id,
	    @jobid AS md_transformation_job_id
	
		   FROM(SELECT 			
			CONVERT(date, CONVERT(datetime, right(document_date,8), 105)) as datekey,
			adj.document_store AS locationKey,
			adj.item_code AS productKey,
			CONCAT(adj.document_store, adj.item_code) AS location_productKey,
			adj.reason_code AS adjustment_code,
			adjr.adjustment_reason AS adjustment_reason,
			case when adj.document_type='SEX' then -1*abs(adj.quantity) else adj.quantity end AS adjustment_units,
			it.cost AS item_cost_aud,
			ISNULL(base.revenue_since_last_stocktake / NULLIF(base.units_since_last_stocktake, 0),0) AS item_rrp_aud	   
			
			FROM [std].[cegid_transactions_adjustments] adj
			LEFT JOIN cte_main base on base.location_code=adj.document_store and base.product_code=adj.item_code
			LEFT JOIN [std].[dimadjustment_reasons] adjr ON adjr.adjustment_code = adj.reason_code
			LEFT JOIN (
					SELECT
					DISTINCT sku_code,
					locationid,
					cost
					FROM
					[std].[dimitem_location]) it ON it.sku_code = base.product_code AND it.locationid = base.location_code
			
			) as main
			
			GROUP BY main.datekey,
		main.locationKey,
		main.productKey,
		main.location_productKey,
        main.adjustment_code,
        main.adjustment_reason,
        main.item_cost_aud,
		main.item_rrp_aud
			
  
--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPSTRINVADJ' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = MAX(md_record_written_timestamp)
FROM
    [cons_retail].[store_inventory_adjustments];

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    cons_retail.store_inventory_adjustments
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
    'cons_retail.sp_store_inventory_adjustments' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
