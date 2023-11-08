SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*****Stored procedure to generate revenue,location,product,reason_code and stocktake details at stocktake_name level*****/
CREATE PROC [std].[sp_store_stocktake] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY 
    IF @reset = 0 
    BEGIN 
    DECLARE @max_ingestion_date_cons [varchar](500)
    SELECT
        @max_ingestion_date_cons = MAX(CAST([md_record_written_timestamp] AS date))
    FROM
        cons_retail.store_stocktake;
    
	
    TRUNCATE TABLE [cons_retail].[store_stocktake];
    
	/***cte_main - to get required revenue and sales details between stocktake_date and last_stocktake_date ***/
    WITH cte_main AS (
      /*fetching required attributes between current and last stock take date */
      SELECT
        pr_1.location_code,
        li_1.product_code,
        SUM(revenue_tax_exc_AUD) AS revenue_since_last_stocktake,
        SUM(li_1.sales_units) AS units_since_last_stocktake
      FROM
        std.purchase_record pr_1
        JOIN std.purchase_record_line_item li_1 ON pr_1.orderid = li_1.orderid
        JOIN std.dimstocktake_schedule sc_1 ON sc_1.locationkey = pr_1.location_code
      WHERE
        CAST(pr_1.create_date_purchase AS date) BETWEEN CAST(sc_1.last_stocktake_date AS date) AND CAST(sc_1.stocktake_date AS date)
      GROUP BY
        pr_1.location_code,
        li_1.product_code
    )
    /*Inserting data to target table ([cons_retail].[store_stocktake])*/
    INSERT INTO [cons_retail].[store_stocktake]
    SELECT
      pr.location_code AS locationkey,
      li.product_code AS productkey,
      CONCAT(pr.location_code, li.product_code) AS location_productKey,
      
      /*stocktake attributes*/
      gs.stocktake_name,
      gs.stocktake_qtr,
      gs.stocktake_year,
      gs.stocktake_date,
      
      /*attributes for units and revenues*/
      SUM(ad.quantity) AS stocktake_variance_units,
      ABS(SUM(ad.quantity)) AS stocktake_variance_units_abs,
      it.cost AS item_cost_aud,
      ISNULL(ct.revenue_since_last_stocktake / NULLIF(ct.units_since_last_stocktake, 0),0) AS item_rrp_aud,
      ct.revenue_since_last_stocktake AS revenue_since_last_stocktake,
      ct.units_since_last_stocktake AS units_since_last_stocktake,
      
      /*last stocktake attributes*/
      gs.last_stocktake_name,
      gs.last_stocktake_qtr,
      gs.last_stocktake_year,
      gs.last_stocktake_date,
      
      /*reason_code attributes*/
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
      reason_code.asls_other,
	  getDate() AS md_record_written_timestamp,
	  @pipelineid AS md_record_written_pipeline_id,
	  @jobid AS md_transformation_job_id
    FROM
      std.purchase_record pr
      LEFT JOIN std.purchase_record_line_item li ON pr.orderid = li.orderid
      LEFT JOIN std.dimstocktake_schedule gs ON gs.locationkey = pr.location_code
      LEFT JOIN std.cegid_transactions_adjustments ad ON ad.document_store = pr.location_code AND ad.item_code = li.product_code 
      LEFT JOIN ( SELECT distinct sku_code,
    					 locationid,
    					 cost
    			  FROM cons_reference.dim_ItemCost_view
    			) it ON it.sku_code = li.product_code AND it.locationid = pr.location_code
      LEFT JOIN cte_main AS ct ON ct.product_code = li.product_code AND ct.location_code = pr.location_code
      /*logic to create attributes for each reason code and to take the sum of quantity*/
      LEFT JOIN(
        SELECT
          main.location_code,
          main.product_code,
          SUM(main.asls_aged_stock) AS asls_aged_stock,
          SUM(main.asls_retail_customer_return) AS asls_retail_customer_return,
          SUM(main.asls_damaged_in_store) AS asls_damaged_in_store,
          SUM(main.asls_damaged_in_transit) AS asls_damaged_in_transit,
          SUM(main.asls_kit_break) AS asls_kit_break,
          SUM(main.asls_PQ_removal_from_sale) AS asls_PQ_removal_from_sale,
          SUM(main.asls_known_theft) AS asls_known_theft,
          SUM(main.asls_covert_to_tester) AS asls_covert_to_tester,
          SUM(main.asls_convert_for_treatment_use) AS asls_convert_for_treatment_use,
          SUM(main.asls_amenity_account_gift) AS asls_amenity_account_gift,
          SUM(main.asls_retail_customer_gift) AS asls_retail_customer_gift,
          SUM(main.asls_product_donation) AS asls_product_donation,
          SUM(main.asls_head_office_authorized) AS asls_head_office_authorized,
          SUM(main.asls_marketing_pr_initiative) AS asls_marketing_pr_initiative,
          SUM(main.asls_staff_complimentary_product) AS asls_staff_complimentary_product,
          SUM(main.asls_online_customer_return) AS asls_online_customer_return,
          SUM(main.asls_online_customer_gift) AS asls_online_customer_gift,
          SUM(main.asls_online_lost_damaged) AS asls_online_lost_damaged,
          SUM(main.asls_overdelivery_from_warehouse) AS asls_overdelivery_from_warehouse,
          SUM(main.asls_underdelivery_from_warehouse) AS asls_underdelivery_from_warehouse,
          SUM(main.asls_stocktake_or_cycle_count_processing_error) AS asls_stocktake_or_cycle_count_processing_error,
          SUM(main.asls_staff_processing_error) AS asls_staff_processing_error,
          SUM(main.asls_online_staff_processing_error) AS asls_online_staff_processing_error,
          SUM(main.asls_new_starter_allocation) AS asls_new_starter_allocation,
          SUM(main.asls_sachet_sample_processing_error) AS asls_sachet_sample_processing_error,
          SUM(main.asls_PQ_employee) AS asls_PQ_employee,
          SUM(main.asls_PQ_customer) AS asls_PQ_customer,
          SUM(main.asls_other) AS asls_other
        FROM
          (
            SELECT
              pr_2.location_code,
              li_2.product_code,
    		  CASE WHEN ad_1.reason_code = 'R01' THEN SUM(ad_1.quantity) END AS asls_aged_stock,
    		  CASE WHEN ad_1.reason_code = 'R02' THEN SUM(ad_1.quantity) END AS asls_retail_customer_return,
    		  CASE WHEN ad_1.reason_code = 'R03' THEN SUM(ad_1.quantity) END AS asls_damaged_in_store,
    		  CASE WHEN ad_1.reason_code = 'R04' THEN SUM(ad_1.quantity) END AS asls_damaged_in_transit,
    		  CASE WHEN ad_1.reason_code = 'R05' THEN SUM(ad_1.quantity) END AS asls_kit_break,
    		  CASE WHEN ad_1.reason_code = 'R06' THEN SUM(ad_1.quantity) END AS asls_PQ_removal_from_sale,
    		  CASE WHEN ad_1.reason_code = 'R07' THEN SUM(ad_1.quantity) END AS asls_known_theft,
    		  CASE WHEN ad_1.reason_code = 'R08' THEN SUM(ad_1.quantity) END AS asls_covert_to_tester,
    		  CASE WHEN ad_1.reason_code = 'R09' THEN SUM(ad_1.quantity) END AS asls_convert_for_treatment_use,
    		  CASE WHEN ad_1.reason_code = 'R11' THEN SUM(ad_1.quantity) END AS asls_amenity_account_gift,
    		  CASE WHEN ad_1.reason_code = 'R12' THEN SUM(ad_1.quantity) END AS asls_retail_customer_gift,
    		  CASE WHEN ad_1.reason_code = 'R13' THEN SUM(ad_1.quantity) END AS asls_product_donation,
    		  CASE WHEN ad_1.reason_code = 'R14' THEN SUM(ad_1.quantity) END AS asls_head_office_authorized,
    		  CASE WHEN ad_1.reason_code = 'R15' THEN SUM(ad_1.quantity) END AS asls_marketing_pr_initiative,
    		  CASE WHEN ad_1.reason_code = 'R16' THEN SUM(ad_1.quantity) END AS asls_staff_complimentary_product,
    		  CASE WHEN ad_1.reason_code = 'R17' THEN SUM(ad_1.quantity) END AS asls_online_customer_return,
    		  CASE WHEN ad_1.reason_code = 'R18' THEN SUM(ad_1.quantity) END AS asls_online_customer_gift,
    		  CASE WHEN ad_1.reason_code = 'R19' THEN SUM(ad_1.quantity) END AS asls_online_lost_damaged,
    		  CASE WHEN ad_1.reason_code = 'R21' THEN SUM(ad_1.quantity) END AS asls_overdelivery_from_warehouse,
    		  CASE WHEN ad_1.reason_code = 'R22' THEN SUM(ad_1.quantity) END AS asls_underdelivery_from_warehouse,
    		  CASE WHEN ad_1.reason_code = 'R23' THEN SUM(ad_1.quantity) END AS asls_stocktake_or_cycle_count_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R24' THEN SUM(ad_1.quantity) END AS asls_staff_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R26' THEN SUM(ad_1.quantity) END AS asls_online_staff_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R28' THEN SUM(ad_1.quantity) END AS asls_new_starter_allocation,
    		  CASE WHEN ad_1.reason_code = 'R29' THEN SUM(ad_1.quantity) END AS asls_sachet_sample_processing_error,
    		  CASE WHEN ad_1.reason_code = 'R30' THEN SUM(ad_1.quantity) END AS asls_PQ_employee,
    		  CASE WHEN ad_1.reason_code = 'R31' THEN SUM(ad_1.quantity) END AS asls_PQ_customer,
    		  CASE WHEN ad_1.reason_code NOT IN ('R01','R02','R03','R04','R05','R06','R07','R08',
    										     'R09','R11','R12','R13','R14','R15','R16','R17',
    											 'R18','R19','R21','R22','R23','R24','R26','R28',
    											 'R29','R30','R31') THEN SUM(ad_1.quantity) END AS asls_other
            FROM
              std.purchase_record pr_2
              LEFT JOIN std.purchase_record_line_item li_2 ON pr_2.orderid = li_2.orderid
              LEFT JOIN std.dimstocktake_schedule gs_1 ON gs_1.locationkey = pr_2.location_code
              LEFT JOIN std.cegid_transactions_adjustments ad_1 ON ad_1.document_store = pr_2.location_code
              AND ad_1.item_code = li_2.product_code
            WHERE
              CAST(pr_2.create_date_purchase AS date) BETWEEN CAST(gs_1.last_stocktake_date AS date) AND CAST(gs_1.stocktake_date AS date)
            GROUP BY
              pr_2.location_code,
              li_2.product_code,
              ad_1.reason_code ) AS main
        GROUP BY
          main.location_code,
          main.product_code ) AS reason_code ON reason_code.location_code = pr.location_code AND reason_code.product_code = li.product_code 
    GROUP BY
      pr.location_code,
      li.product_code,
      gs.stocktake_name,
      gs.stocktake_qtr,
      gs.stocktake_year,
      gs.stocktake_date,
      gs.last_stocktake_name,
      gs.last_stocktake_qtr,
      gs.last_stocktake_year,
      gs.last_stocktake_date,
      CONCAT(pr.location_code, li.product_code),
      it.cost,
      ct.revenue_since_last_stocktake,
      ct.units_since_last_stocktake,
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
    cons_retail.store_stocktake;

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    cons_retail.store_stocktake
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
    'cons_retail.store_stocktake' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END