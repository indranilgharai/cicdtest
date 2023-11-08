/****** Object:  StoredProcedure [std].[sp_offcycle_stock]    Script Date: 12/14/2022 11:51:16 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/***Stored procedure to add the offcycle data to dimstocktake_schedule and to delete the existing location codes***/
CREATE PROC [std].[sp_offcycle_stock] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS BEGIN BEGIN TRY 
    IF @reset = 0 
    BEGIN 
    DECLARE @max_ingestion_date_cons [varchar](500)
    SELECT
        @max_ingestion_date_cons = MAX(CAST([md_record_written_timestamp] AS date))
    FROM
        [std].[dimstocktake_schedule];

	/***Logic to delete the existing records in dimstocktake_schedule that matches with offcycle***/
	DELETE FROM [std].[dimstocktake_schedule] where locationkey IN ( SELECT distinct location_code
																	 FROM stage.offcycle_stocktake offstk
																	 JOIN [std].[store_x] st 
																	 ON st.store_name=offstk.[store_name]);
	/*Inserting the data into the main table*/
	INSERT INTO [std].[dimstocktake_schedule](
		locationkey,
		source_system,
		stocktake_name,
		stocktake_qtr,
		stocktake_year,
		stocktake_date,
		md_record_written_timestamp,
		md_record_written_pipeline_id,
		md_transformation_job_id)
	SELECT st.location_code AS locationkey
	,CAST(offstk.POS AS varchar(20)) AS source_system
	,CAST(CASE 
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 1 AND 3 THEN concat('Q1 ',DATENAME(month,CAST(offstk.stocktake_time AS date)),' ',YEAR(CAST(offstk.stocktake_time AS date)),' Offcycle')
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 4 AND 6 THEN concat('Q2 ',DATENAME(month,CAST(offstk.stocktake_time AS date)),' ',YEAR(CAST(offstk.stocktake_time AS date)),' Offcycle')
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 7 AND 9 THEN concat('Q3 ',DATENAME(month,CAST(offstk.stocktake_time AS date)),' ',YEAR(CAST(offstk.stocktake_time AS date)),' Offcycle')
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 10 AND 12 THEN concat('Q4 ',DATENAME(month,CAST(offstk.stocktake_time AS date)),' ',YEAR(CAST(offstk.stocktake_time AS date)),' Offcycle')
		  END AS varchar(50)) AS stocktake_name
	
	
	
	/*logic for calculating quarter*/
	,CAST(CASE 
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 1 AND 3 THEN 'Q1'
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 4 AND 6 THEN 'Q2'
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 7 AND 9 THEN 'Q3'
		  WHEN MONTH(CAST(offstk.stocktake_time AS date)) BETWEEN 10 AND 12 THEN 'Q4'
		  END AS varchar(5)) AS stocktake_qtr
	,CAST(year(CAST(offstk.stocktake_time AS date)) AS int) AS stocktake_year
	,CAST(offstk.stocktake_time AS date) AS stocktake_date
	,getDate() AS md_record_written_timestamp
	,@pipelineid AS md_record_written_pipeline_id
	,@jobid AS md_transformation_job_id
	
	FROM (
			SELECT region,
			       subsidiary,
			       [store_name] AS store_name,
			       POS,
			       [stocktake_time (local)] AS stocktake_name,
			       [Stocktake_Time] AS stocktake_time
			FROM stage.offcycle_stocktake) AS offstk
	JOIN [std].[store_x] st ON st.store_name=offstk.store_name;
	
	
	
--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
DECLARE @label VARCHAR(500)
SET
    @label = 'AADPOFFCYCSTK' EXEC meta_ctl.sp_row_count @jobid,
    @step_number,
    @label
    
    TRUNCATE TABLE stage.offcycle_stocktake;
    
END
ELSE BEGIN DECLARE @newrec DATETIME,
@onlydate DATE
SELECT
    @newrec = MAX(md_record_written_timestamp)
FROM
    [std].[dimstocktake_schedule];

SELECT
    @onlydate = CAST(@newrec AS DATE);

DELETE FROM
    [std].[dimstocktake_schedule]
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
    'std.sp_offcycle_stock' AS ErrorProcedure,
    ERROR_MESSAGE() AS ErrorMessage,
    getdate() AS Updated_date
END CATCH
END
