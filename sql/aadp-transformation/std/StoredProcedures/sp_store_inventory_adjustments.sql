/****** Object:  StoredProcedure [std].[sp_Store_Inventory_Adjustments]    Script Date: 12/13/2022 4:37:36 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*****Stored procedure to generate revenue,location,product,adjustment_reason and unit details*****/
CREATE PROC [std].[sp_store_inventory_adjustments] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY 
    IF @reset = 0 
    BEGIN 
    DECLARE @max_ingestion_date_cons [varchar](500)
    SELECT
        @max_ingestion_date_cons = MAX(CAST([md_record_written_timestamp] AS date))
    FROM
        cons_retail.store_inventory_adjustments;
    
	
    TRUNCATE TABLE cons_retail.store_inventory_adjustments;
    
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
    /*Inserting data to target table ([cons_retail].[store_inventory_adjustments])*/
    INSERT INTO
      cons_retail.store_inventory_adjustments
    /*fetching the required attributes*/
    SELECT
      DISTINCT CAST(pr.create_date_purchase AS date) AS dateKey,
      pr.location_code AS locationKey,
      li.product_code AS productKey,
      CONCAT(pr.location_code, li.product_code) AS location_productKey,
      adj.reason_code AS adjustment_code,
      rc.reason_code_description AS adjustment_reason,
      adj.quantity AS adjustment_units,
      it.cost AS item_cost_aud,
      ISNULL(ct.revenue_since_last_stocktake / NULLIF(ct.units_since_last_stocktake, 0),0) AS item_rrp_aud,
	  getDate() AS md_record_written_timestamp,
	  @pipelineid AS md_record_written_pipeline_id,
	  @jobid AS md_transformation_job_id
    FROM
      std.purchase_record pr
      LEFT JOIN std.purchase_record_line_item li ON pr.orderid = li.orderid
      LEFT JOIN std.cegid_transactions_adjustments adj ON li.product_code = adj.item_code
      AND pr.location_code = adj.document_store
      LEFT JOIN std.reason_code rc ON rc.reason_code = adj.reason_code
      LEFT JOIN (
        SELECT
          DISTINCT sku_code,
          locationid,
          cost
        FROM
          cons_reference.dim_ItemCost_view) it ON it.sku_code = li.product_code AND it.locationid = pr.location_code
      LEFT JOIN cte_main AS ct ON ct.product_code = li.product_code AND ct.location_code = pr.location_code;
      
  
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
    cons_retail.store_inventory_adjustments;

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
    'cons_retail.store_inventory_adjustments' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END