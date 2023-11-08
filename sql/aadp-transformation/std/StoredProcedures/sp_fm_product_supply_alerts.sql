-- ## SP for load of Standardised table : PRODUCT_SUPPLY_ALERTS ##
--Modified Script [28/06/2022]: Removed few filters while loadig data from [stage].[fm_dp]
--Modified Script [12/08/2022]: changed update statement and update condition to correct the eff_from_dt and eff_to_dt
--Modified Script [24/08/2022]: changed insert condition for change in existing measures
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_fm_product_supply_alerts] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		--Fetching the latest snapshot from fm table ------
		DECLARE @max_ingestion_date [varchar](500)
		select @max_ingestion_date=max(CAST(convert(datetime,[md_record_ingestion_timestamp],103) as datetime )) from stage.fm_drp
		
		DECLARE @load_strt_time [datetime]
		SET @load_strt_time=getdate();

		IF OBJECT_ID('tempdb..#product_supply_alerts_temp') IS NOT NULL
		BEGIN
			DROP TABLE #product_supply_alerts_temp
		END
		-----------------------temporary table to load the current snapshot data ---------------------------

		create table #product_supply_alerts_temp
		WITH
		(
		 DISTRIBUTION = ROUND_ROBIN
		 ,HEAP
		)
		AS
		select drp.SKUCODE as sku_code,
		drp.LOCATIONCODE as location_code,
		CAST(CONVERT(DATE, md_record_ingestion_timestamp, 103) AS DATE) as forecast_date,
		--DATEFROMPARTS(year(CONVERT(DATE, drp.startdateofweek, 103)),month(CONVERT(DATE, drp.startdateofweek, 103)),'01') as forecast_period,
		drp.startdateofweek as forecast_period,
		drp.stock_on_hand as stock_on_hand,
		drp.safety_stock as safety_stock,
		drp.stock_coverage_no_of_weeks_for_soh as stock_coverage_no_of_weeks_for_soh,
		drp.purchase_order as purchase_order,
		drp.goods_in_transit as goods_in_transit,
		drp.planned_receipt as planned_receipt,
		drp.planned_available as planned_available,
		CASE WHEN (( abs(cast(drp.GROSS_REQUIREMENT as int)) - abs(cast(drp.STOCK_ON_HAND as int)) ) < 1 ) THEN 'Y'
		ELSE 'N' END out_of_stock_flag,
		drp.gross_requirement as gross_requirement,
		drp.WEEK as week,
		drp.startdateofweek,
		--- change it CAST(GETDATE() AS DATE) as eff_from_dt,
		--- changing eff_from_dt as same as forecast date
		CAST(CONVERT(DATE, md_record_ingestion_timestamp, 103) AS DATE) as eff_from_dt, 
		CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE) as eff_to_dt, 
		CAST(CONVERT(DATETIME, [md_record_ingestion_timestamp], 103) AS DATETIME) AS  md_record_ingestion_timestamp,
		CAST([md_record_ingestion_pipeline_id] AS VARCHAR(200)) AS [md_record_ingestion_pipeline_id],
		CAST([md_source_system] AS VARCHAR(100)) AS [md_source_system],
		getdate() AS [md_record_written_timestamp],
		@pipelineid as [md_record_written_pipeline_id],
		@jobid as [md_transformation_job_id]
		from stage.fm_drp drp where  CAST(convert(datetime,[md_record_ingestion_timestamp],103) as datetime ) = @max_ingestion_date 
		AND [GROSS_REQUIREMENT] is not null 
		and [STOCK_ON_HAND] <> '0' 
		and [SAFETY_STOCK] <> '0'
		and [STOCK_COVERAGE_NO_OF_WEEKS_FOR_SOH] <> '0.00' 
		--and [PURCHASE_ORDER] is not null 
		--and [GOODS_IN_TRANSIT] is not null 
		--and [PLANNED_RECEIPT] is not null 
		--and [PLANNED_AVAILABLE] is not null 
		and startdateofweek <> '01/00/0';
		 
		DECLARE @read_count [int]
		select @read_count=count(*) from #product_supply_alerts_temp;
		 
		 --------- UPDATE product_supply_alerts  with eff_to_dt as current_date for existing matched keys and with a change in any column value ----------
		IF  @read_count > 0
		BEGIN
			 
			DECLARE @update_rec [int]
			select @update_rec=count(*) from #product_supply_alerts_temp
			 INNER JOIN std.fm_product_supply_alerts tgt ON (tgt.sku_code = #product_supply_alerts_temp.sku_code AND tgt.location_code = #product_supply_alerts_temp.location_code AND tgt.week = #product_supply_alerts_temp.week AND tgt.startdateofweek = #product_supply_alerts_temp.startdateofweek) 
			 WHERE (#product_supply_alerts_temp.[stock_on_hand] <> tgt.[stock_on_hand]
			OR #product_supply_alerts_temp.[safety_stock] <> tgt.[safety_stock]
			OR #product_supply_alerts_temp.[stock_coverage_no_of_weeks_for_soh] <> tgt.[stock_coverage_no_of_weeks_for_soh]
			OR #product_supply_alerts_temp.[purchase_order] <> tgt.[purchase_order]
			OR #product_supply_alerts_temp.[goods_in_transit] <> tgt.[goods_in_transit]
			OR #product_supply_alerts_temp.[planned_receipt] <> tgt.[planned_receipt]
			OR #product_supply_alerts_temp.[planned_available] <> tgt.[planned_available]
			OR #product_supply_alerts_temp.[gross_requirement] <> tgt.[gross_requirement])
			and tgt.eff_to_dt=CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE);
			PRINT @update_rec
			if @update_rec>0
			BEGIN
				UPDATE std.fm_product_supply_alerts
				--- change it CAST(GETDATE() AS DATE),
				--- changing eff_to_dt to incoming latest record's effective from date
				SET eff_to_dt = #product_supply_alerts_temp.[eff_from_dt]
				from #product_supply_alerts_temp
				INNER JOIN std.fm_product_supply_alerts tgt ON (tgt.sku_code = #product_supply_alerts_temp.sku_code AND tgt.location_code = #product_supply_alerts_temp.location_code AND tgt.week = #product_supply_alerts_temp.week AND tgt.startdateofweek = #product_supply_alerts_temp.startdateofweek) 
				WHERE (#product_supply_alerts_temp.[stock_on_hand] <> tgt.[stock_on_hand]
				OR #product_supply_alerts_temp.[safety_stock] <> tgt.[safety_stock]
				OR #product_supply_alerts_temp.[stock_coverage_no_of_weeks_for_soh] <> tgt.[stock_coverage_no_of_weeks_for_soh]
				OR #product_supply_alerts_temp.[purchase_order] <> tgt.[purchase_order]
				OR #product_supply_alerts_temp.[goods_in_transit] <> tgt.[goods_in_transit]
				OR #product_supply_alerts_temp.[planned_receipt] <> tgt.[planned_receipt]
				OR #product_supply_alerts_temp.[planned_available] <> tgt.[planned_available]
				OR #product_supply_alerts_temp.[gross_requirement] <> tgt.[gross_requirement])
				and tgt.eff_to_dt=CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE);

				----------------------insert new records into product_supply_alerts for existing matched keys with a change in any column value-----------------
				
				INSERT INTO std.fm_product_supply_alerts 
				SELECT src.sku_code as sku_code,
				src. location_code as location_code,
				src.forecast_date as forecast_date,
				src.forecast_period as forecast_period,
				src.stock_on_hand as stock_on_hand,
				src.safety_stock as safety_stock,
				src.stock_coverage_no_of_weeks_for_soh as stock_coverage_no_of_weeks_for_soh,
				src.purchase_order as purchase_order,
				src.goods_in_transit as goods_in_transit,
				src.planned_receipt as planned_receipt,
				src.planned_available as planned_available,
				src.out_of_stock_flag as out_of_stock_flag,
				src.gross_requirement as gross_requirement,
				src.week as week,
				src.startdateofweek as startdateofweek,
				src.eff_from_dt as eff_from_dt,
				src.eff_to_dt as eff_to_dt,
				src.md_record_ingestion_timestamp as md_record_ingestion_timestamp,
				src.md_record_ingestion_pipeline_id as md_record_ingestion_pipeline_id,
				src.md_source_system as md_source_system,
				src.md_record_written_timestamp as md_record_written_timestamp,
				src.md_record_written_pipeline_id as md_record_written_pipeline_id,
				src.md_transformation_job_id as md_transformation_job_id
				FROM #product_supply_alerts_temp src
				INNER JOIN std.fm_product_supply_alerts tgt ON (tgt.sku_code = src.sku_code AND tgt.location_code = src.location_code AND tgt.week = src.week AND tgt.startdateofweek = src.startdateofweek) 
				WHERE (src.[stock_on_hand] <> tgt.[stock_on_hand]
				OR src.[safety_stock] <> tgt.[safety_stock]
				OR src.[stock_coverage_no_of_weeks_for_soh] <> tgt.[stock_coverage_no_of_weeks_for_soh]
				OR src.[purchase_order] <> tgt.[purchase_order]
				OR src.[goods_in_transit] <> tgt.[goods_in_transit]
				OR src.[planned_receipt] <> tgt.[planned_receipt]
				OR src.[planned_available] <> tgt.[planned_available]
				OR src.[gross_requirement] <> tgt.[gross_requirement])
				-- changing insert condition for change in existing measures
				and (tgt.eff_to_dt=CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE) or tgt.eff_to_dt=src.[eff_from_dt]); 
			END

			------------------insert completely new records which have come in new snapshots -------------

			INSERT INTO std.fm_product_supply_alerts 
			SELECT src.sku_code as sku_code,
			src. location_code as location_code,
			src.forecast_date as forecast_date,
			src.forecast_period as forecast_period,
			src.stock_on_hand as stock_on_hand,
			src.safety_stock as safety_stock,
			src.stock_coverage_no_of_weeks_for_soh as stock_coverage_no_of_weeks_for_soh,
			src.purchase_order as purchase_order,
			src.goods_in_transit as goods_in_transit,
			src.planned_receipt as planned_receipt,
			src.planned_available as planned_available,
			src.out_of_stock_flag as out_of_stock_flag,
			src.gross_requirement as gross_requirement,
			src.week as week,
			src.startdateofweek as startdateofweek,
			src.eff_from_dt as eff_from_dt,
			src.eff_to_dt as eff_to_dt,
			src.md_record_ingestion_timestamp as md_record_ingestion_timestamp,
			src.md_record_ingestion_pipeline_id as md_record_ingestion_pipeline_id,
			src.md_source_system as md_source_system,
			src.md_record_written_timestamp as md_record_written_timestamp,
			src.md_record_written_pipeline_id as md_record_written_pipeline_id,
			src.md_transformation_job_id as md_transformation_job_id
			FROM #product_supply_alerts_temp src
			LEFT OUTER JOIN std.fm_product_supply_alerts tgt ON (tgt.sku_code = src.sku_code AND tgt.location_code = src.location_code AND tgt.week = src.week AND tgt.startdateofweek = src.startdateofweek) WHERE tgt.sku_code is null or tgt.location_code is null or tgt.week is null or tgt.startdateofweek is null and tgt.eff_to_dt=CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE);
			---	removing the logic deletion of records from product_supply_alerts which are not present in current snapshot----

			UPDATE STATISTICS std.fm_product_supply_alerts;
			UPDATE STATISTICS stage.fm_drp;
			 
			 
		END
		DECLARE @write_count [int]
		select @write_count=count(*) from std.fm_product_supply_alerts where md_record_written_timestamp>=@load_strt_time;
		
		insert [meta_ctl].[transform_count_record_table] select (select @jobid),(select @step_number),(select @read_count),(select @write_count),(select getdate());


	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:std.fm_product_supply_alerts'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_fm_product_supply_alerts' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END