SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*****Stored procedure to generate revenue,location,product,reason_code and stocktake details at stocktake_name level*****/

/*****Modified Stored procedure to generate revenue,location,product,reason_code and stocktake details at stocktake_name level on 09 May 2023*****/

CREATE PROC [cons_retail].[sp_store_stocktake] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY 
    IF @reset = 0 
    BEGIN 
    DECLARE @max_ingestion_date_cons [varchar](500)
    SELECT
        @max_ingestion_date_cons = MAX(CAST([md_record_written_timestamp] AS date)) FROM
        [cons_retail].[store_stocktake];
    
	
    TRUNCATE TABLE [cons_retail].[store_stocktake];
    
/***cte_main - to get required revenue and sales details between stocktake_date and last_stocktake_date ***/
	
	
	
    WITH cte_main AS (
/*fetching required attributes between current and last stock take date */
/*Modified the SP to include to match unit variance with global stocktake report */
   SELECT locationkey,
   sku_code,		
   stocktake_name,		
   stocktake_qtr,		
   stocktake_year,		
   stocktake_date,		
   last_stocktake_name,		
   last_stocktake_qtr,		
   last_stocktake_year,		
   last_stocktake_date,						
   SUM(revenue_since_last_stocktake) as revenue_since_last_stocktake,		
   SUM(units_since_last_stocktake) as units_since_last_stocktake,
   SUM(quantity) as quantity   
   FROM 			
   (
   SELECT				
   sc.locationkey,				
   sales.create_date_purchase,				
   dl.sku_code,				
   revenue_tax_exc_AUD AS revenue_since_last_stocktake,				
   sales.sales_units AS units_since_last_stocktake,												
   sc.stocktake_name,				
   sc.stocktake_qtr,				
   sc.stocktake_year,				
   sc.stocktake_date,										
   sc.last_stocktake_name,				
   sc.last_stocktake_qtr,				
   sc.last_stocktake_year,				
   sc.last_stocktake_date,
   cde.quantity   
   FROM				
  [std].[dimstocktake_schedule] sc
  LEFT JOIN [std].[dimitem_location] dl ON dl.locationID =   sc.locationkey and active_record = 1
  LEFT JOIN [std].[cegid_transactions_adjustments] cde ON cde.document_store = sc.locationkey 
  and cde.item_code = dl.sku_code and cde.document_type = 'INV'									
  AND CONVERT(date, CONVERT(datetime, right(cde.document_date,8), 105))= sc.stocktake_date												
  LEFT JOIN (				
  Select location_code,				
  product_code,				
  cast(create_date_purchase as date) as create_date_purchase ,				
  sum(revenue_tax_exc_aud) as revenue_tax_exc_aud ,				
  sum(sales_units) as sales_units				
  FROM					
  (
  SELECT case when order_type = 'ClickandCollect' THEN (	
  CASE WHEN pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)												
  WHEN pr.source_system = 'RETAILPRO' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999)												
  WHEN pr.source_system = 'HYBRIS' THEN CAST( ISNULL( pr.fulfillment_location_code, ISNULL(pr.location_code, '999')) AS VARCHAR(50))	
  ELSE NULL END )										
  ELSE (CASE WHEN pr.source_system = 'CEGID' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999) WHEN pr.source_system = 'RETAILPRO' THEN ISNULL
  (CAST(pr.location_code AS VARCHAR(50)), 999)												
  WHEN pr.source_system = 'HYBRIS' THEN ISNULL(CAST(pr.location_code AS VARCHAR(50)), 999) ELSE NULL END ) END location_code,					
  product_code,					
  CAST(CASE WHEN (pr.channel_id = 'Digital') THEN CASE WHEN left(pr.orderid, 1) = 'H' 					
  THEN CASE WHEN pr.source_system = 'HYBRIS'												
  AND (pr.OrderStatus = 'SHIPPED' or pr.OrderStatus = 'DELIVERED' or pr.OrderStatus = 'COMPLETED' or pr.OrderStatus = 'RETURNED') 
  THEN shipped_date															
  ELSE NULL END ELSE pr.create_date_purchase END ELSE pr.create_date_purchase END AS DATE) create_date_purchase,					
  revenue_tax_exc_aud,					
  sales_units					
  from std.purchase_record pr						
  JOIN (select * from [std].[purchase_record_line_item]  WHERE							
  ( UPPER(cancelled_flag) IN ('N') OR cancelled_flag IS NULL ) 																																				
  and orderid not in (select orderid from	std.purchase_record_line_item 								
  where product_code = 'CLICKCOLLECT'))as li ON pr.orderid = li.orderid								
  where pr.location_code is not null and li.product_code is not null								
  ) abc								
  group by 								
  location_code,												
  product_code,												
  cast(create_date_purchase as date)								
  ) sales								
  ON sales.location_code = sc.locationkey								
  and dl.sku_code = sales.product_code								
  and CAST(sales.create_date_purchase AS date) BETWEEN DATEADD(DAY,1,CAST(sc.last_stocktake_date AS date)) AND CAST(sc.stocktake_date AS date)
   /*JOIN (						
  SELECT * FROM std.product_x						
  WHERE							
  product_type_sub_cat in ('Retail', 'Kit Item Only')							
  and category not in ('Non Sale', 'Packaging Component', 'Voucher')					
  ) prd on li.product_code=prd.description1*/
  
  ) main
  group by locationkey,			
  sku_code,		
  stocktake_name,		
  stocktake_qtr,		
  stocktake_year,		
  stocktake_date,		
  last_stocktake_name,		
  last_stocktake_qtr,		
  last_stocktake_year,		
  last_stocktake_date	
  having SUM(isnull(units_since_last_stocktake,0)) <> '0'
  or SUM(isnull(quantity,0)) <>0
  )
/*Inserting data to target table ([cons_retail].[store_stocktake])*/
    INSERT INTO [cons_retail].[store_stocktake]
    SELECT DISTINCT
    base.locationkey AS locationkey,
    base.sku_code AS productkey,
    CONCAT(base.locationkey, base.sku_code) AS location_productKey,
/*stocktake attributes*/
    base.stocktake_name,
    base.stocktake_qtr,
    base.stocktake_year,
    base.stocktake_date,
/*attributes for units and revenues*/
    SUM(ad.quantity) AS stocktake_variance_units,
    SUM(ABS(ad.quantity)) AS stocktake_variance_units_abs,
    it.cost AS item_cost_aud,
    ISNULL(base.revenue_since_last_stocktake / NULLIF(base.units_since_last_stocktake, 0),0) AS item_rrp_aud,
    base.revenue_since_last_stocktake as revenue_since_last_stocktake,
    base.units_since_last_stocktake as units_since_last_stocktake,
  /*last stocktake attributes*/
    base.last_stocktake_name,
    base.last_stocktake_qtr,
    base.last_stocktake_year,
    base.last_stocktake_date,
 /*reason_code attributes*/reason_code.asls_aged_stock,
    reason_code.asls_retail_customer_return,
    reason_code.asls_damaged_in_store,
    reason_code.asls_damaged_in_transit,
    reason_code.asls_kit_break,
    reason_code.asls_PQ_removal_from_sale,
    reason_code.asls_known_theft,
    reason_code.asls_covert_to_tester,
    reason_code.asls_convert_for_treatment_use,
    reason_code.asls_amenity_account_gift,
    reason_code.asls_retail_customer_gift,
    reason_code.asls_product_donation,
    reason_code.asls_head_office_authorized,
    reason_code.asls_marketing_pr_initiative,
    reason_code.asls_staff_complimentary_product,
    reason_code.asls_online_customer_return,
    reason_code.asls_online_customer_gift,
    reason_code.asls_online_lost_damaged,
    reason_code.asls_overdelivery_from_warehouse,
    reason_code.asls_underdelivery_from_warehouse,
    reason_code.asls_stocktake_or_cycle_count_processing_error,
    reason_code.asls_staff_processing_error,
    reason_code.asls_online_staff_processing_error,
    reason_code.asls_new_starter_allocation,
    reason_code.asls_sachet_sample_processing_error,
    reason_code.asls_PQ_employee,
    reason_code.asls_PQ_customer,
    reason_code.asls_other,	     
    getDate() AS md_record_written_timestamp,
    @pipelineid AS md_record_written_pipeline_id, 
    @jobid AS md_transformation_job_id	 
FROM cte_main base
LEFT JOIN [std].[cegid_transactions_adjustments] ad ON ad.document_store = base.locationkey
							  AND ad.item_code = base.sku_code
							  AND CONVERT(date, CONVERT(datetime, right(ad.document_date,8), 105))=base.stocktake_date
							  and ad.document_type = 'INV'
													  
LEFT JOIN ( SELECT distinct sku_code,
    			locationid,
    			cost
    		FROM [std].[dimitem_location] where active_record = 1
    			) it ON it.sku_code = base.sku_code AND it.locationid = base.locationkey
				
LEFT JOIN(
        SELECT
          sub.location_code,
          sub.product_code,
          SUM(sub.asls_aged_stock) AS asls_aged_stock,
          SUM(sub.asls_retail_customer_return) AS asls_retail_customer_return,
          SUM(sub.asls_damaged_in_store) AS asls_damaged_in_store,
          SUM(sub.asls_damaged_in_transit) AS asls_damaged_in_transit,
          SUM(sub.asls_kit_break) AS asls_kit_break,
          SUM(sub.asls_PQ_removal_from_sale) AS asls_PQ_removal_from_sale,
          SUM(sub.asls_known_theft) AS asls_known_theft,
          SUM(sub.asls_covert_to_tester) AS asls_covert_to_tester,
          SUM(sub.asls_convert_for_treatment_use) AS asls_convert_for_treatment_use,
          SUM(sub.asls_amenity_account_gift) AS asls_amenity_account_gift,
          SUM(sub.asls_retail_customer_gift) AS asls_retail_customer_gift,
          SUM(sub.asls_product_donation) AS asls_product_donation,
          SUM(sub.asls_head_office_authorized) AS asls_head_office_authorized,
          SUM(sub.asls_marketing_pr_initiative) AS asls_marketing_pr_initiative,
          SUM(sub.asls_staff_complimentary_product) AS asls_staff_complimentary_product,
          SUM(sub.asls_online_customer_return) AS asls_online_customer_return,
          SUM(sub.asls_online_customer_gift) AS asls_online_customer_gift,
          SUM(sub.asls_online_lost_damaged) AS asls_online_lost_damaged,
          SUM(sub.asls_overdelivery_from_warehouse) AS asls_overdelivery_from_warehouse,
          SUM(sub.asls_underdelivery_from_warehouse) AS asls_underdelivery_from_warehouse,
          SUM(sub.asls_stocktake_or_cycle_count_processing_error) AS asls_stocktake_or_cycle_count_processing_error,
          SUM(sub.asls_staff_processing_error) AS asls_staff_processing_error,
          SUM(sub.asls_online_staff_processing_error) AS asls_online_staff_processing_error,
          SUM(sub.asls_new_starter_allocation) AS asls_new_starter_allocation,
          SUM(sub.asls_sachet_sample_processing_error) AS asls_sachet_sample_processing_error,
          SUM(sub.asls_PQ_employee) AS asls_PQ_employee,
          SUM(sub.asls_PQ_customer) AS asls_PQ_customer,
          SUM(sub.asls_other) AS asls_other
        FROM
          (
            SELECT
              pr_2.location_code,
              li_2.product_code,
    		  CASE WHEN ad_1.reason_code = 'R01' THEN ad_1.quantity END AS asls_aged_stock,
    		  CASE WHEN ad_1.reason_code = 'R02' THEN ad_1.quantity END AS asls_retail_customer_return,
    		  CASE WHEN ad_1.reason_code = 'R03' THEN ad_1.quantity END AS asls_damaged_in_store,
    		  CASE WHEN ad_1.reason_code = 'R04' THEN ad_1.quantity END AS asls_damaged_in_transit,
    		  CASE WHEN ad_1.reason_code = 'R05' THEN ad_1.quantity END AS asls_kit_break,
    		  CASE WHEN ad_1.reason_code = 'R06' THEN ad_1.quantity END AS asls_PQ_removal_from_sale,
    		  CASE WHEN ad_1.reason_code = 'R07' THEN ad_1.quantity END AS asls_known_theft,
    		  CASE WHEN ad_1.reason_code = 'R08' THEN ad_1.quantity END AS asls_covert_to_tester,
    		  CASE WHEN ad_1.reason_code = 'R09' THEN ad_1.quantity END AS asls_convert_for_treatment_use,
    		  CASE WHEN ad_1.reason_code = 'R11' THEN ad_1.quantity END AS asls_amenity_account_gift,
    		  CASE WHEN ad_1.reason_code = 'R12' THEN ad_1.quantity END AS asls_retail_customer_gift,
    		  CASE WHEN ad_1.reason_code = 'R13' THEN ad_1.quantity END AS asls_product_donation,
    		  CASE WHEN ad_1.reason_code = 'R14' THEN ad_1.quantity END AS asls_head_office_authorized,
    		  CASE WHEN ad_1.reason_code = 'R15' THEN ad_1.quantity END AS asls_marketing_pr_initiative,
    		  CASE WHEN ad_1.reason_code = 'R16' THEN ad_1.quantity END AS asls_staff_complimentary_product,
    		  CASE WHEN ad_1.reason_code = 'R17' THEN ad_1.quantity END AS asls_online_customer_return,
    		  CASE WHEN ad_1.reason_code = 'R18' THEN ad_1.quantity END AS asls_online_customer_gift,
    		  CASE WHEN ad_1.reason_code = 'R19' THEN ad_1.quantity END AS asls_online_lost_damaged,
    		  CASE WHEN ad_1.reason_code = 'R21' THEN ad_1.quantity END AS asls_overdelivery_from_warehouse,
    		  CASE WHEN ad_1.reason_code = 'R22' THEN ad_1.quantity END AS asls_underdelivery_from_warehouse,
    		  CASE WHEN ad_1.reason_code = 'R23' THEN ad_1.quantity END AS asls_stocktake_or_cycle_count_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R24' THEN ad_1.quantity END AS asls_staff_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R26' THEN ad_1.quantity END AS asls_online_staff_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R28' THEN ad_1.quantity END AS asls_new_starter_allocation,
    		  CASE WHEN ad_1.reason_code = 'R29' THEN ad_1.quantity END AS asls_sachet_sample_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R30' THEN ad_1.quantity END AS asls_PQ_employee,
    		  CASE WHEN ad_1.reason_code = 'R31' THEN ad_1.quantity END AS asls_PQ_customer,
    		  CASE WHEN ad_1.reason_code NOT IN ('R01','R02','R03','R04','R05','R06','R07','R08',
    										     'R09','R11','R12','R13','R14','R15','R16','R17',
    											 'R18','R19','R21','R22','R23','R24','R26','R28',
    											 'R29','R30','R31') THEN ad_1.quantity END AS asls_other
            FROM
              [std].[purchase_record] pr_2
              LEFT JOIN [std].[purchase_record_line_item] li_2 ON pr_2.orderid = li_2.orderid
              LEFT JOIN [std].[dimstocktake_schedule] gs_1 ON gs_1.locationkey = pr_2.location_code
              LEFT JOIN [std].[cegid_transactions_adjustments] ad_1 ON ad_1.document_store = pr_2.location_code
              AND ad_1.item_code = li_2.product_code
            WHERE
              CAST(pr_2.create_date_purchase AS date) BETWEEN DATEADD(DAY,1,CAST(gs_1.last_stocktake_date AS date)) AND CAST(gs_1.stocktake_date AS date)
            GROUP BY
              pr_2.location_code,
              li_2.product_code,
              ad_1.reason_code,
			  ad_1.quantity) AS sub
            GROUP BY
              sub.location_code,
              sub.product_code ) AS reason_code ON reason_code.location_code = base.locationkey AND reason_code.product_code = base.sku_code
				
	--WHERE base.location_code='01056' and base.stocktake_name = 'Q3 July 2022'
       GROUP BY
	 base.locationkey,
	 base.sku_code,
	 base.stocktake_name,
	 base.stocktake_qtr,
	 base.stocktake_year,
	 base.stocktake_date,
         it.cost,
	 base.revenue_since_last_stocktake,
	 base.units_since_last_stocktake,
	 base.last_stocktake_name,
	 base.last_stocktake_qtr,
	 base.last_stocktake_year,
	 base.last_stocktake_date,
	 reason_code.asls_aged_stock,
         reason_code.asls_retail_customer_return,
         reason_code.asls_damaged_in_store,
         reason_code.asls_damaged_in_transit,
         reason_code.asls_kit_break,
         reason_code.asls_PQ_removal_from_sale,
         reason_code.asls_known_theft,
         reason_code.asls_covert_to_tester,
         reason_code.asls_convert_for_treatment_use,
         reason_code.asls_amenity_account_gift,
         reason_code.asls_retail_customer_gift,
         reason_code.asls_product_donation,
         reason_code.asls_head_office_authorized,
         reason_code.asls_marketing_pr_initiative,
         reason_code.asls_staff_complimentary_product,
         reason_code.asls_online_customer_return,
         reason_code.asls_online_customer_gift,
         reason_code.asls_online_lost_damaged,
         reason_code.asls_overdelivery_from_warehouse,
         reason_code.asls_underdelivery_from_warehouse,
         reason_code.asls_stocktake_or_cycle_count_processing_error,
         reason_code.asls_staff_processing_error,
         reason_code.asls_online_staff_processing_error,
         reason_code.asls_new_starter_allocation,
         reason_code.asls_sachet_sample_processing_error,
         reason_code.asls_PQ_employee,
         reason_code.asls_PQ_customer,
         reason_code.asls_other;

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPSTRSTK' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = max(md_record_written_timestamp)
FROM
    [cons_retail].[store_stocktake];

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    [cons_retail].[store_stocktake]
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
    'cons_retail.sp_store_stocktake' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END

