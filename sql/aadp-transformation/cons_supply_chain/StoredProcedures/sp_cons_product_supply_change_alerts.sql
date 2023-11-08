/****** Modified StoredProcedure [cons_supply_chain].[sp_cons_product_supply_change_alerts] to correct the decimal points for stock on hand
    Modified Date: 7/01/2022 9:30:34 AM Modified by: Harsha Varadhi ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_supply_chain].[sp_cons_product_supply_change_alerts] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			TRUNCATE TABLE [cons_supply_chain].[product_supply_change_alerts];

       
			INSERT INTO [cons_supply_chain].[product_supply_change_alerts]
			select distinct ps.sku_code as sku,
			item.description as sku_description,
			lctn.name as location,
			ps.location_code as inventory_location_code,
			coalesce(lctn.custrecord1,lctn.store_type) as inventory_location_name,
			ps.forecast_date as forecast_date,
			CAST(CONVERT(DATE, ps.forecast_period, 103) AS DATE) as forecast_period,
			dimdate.month_id as month_number,
			dimdate.week_of_year as week_of_year,
			dimdate.yearval as year,
			subx.sbs_region as region,
			ic.name as product_category,
			plc.plc_status as product_life_cycle,
			item.custitem_ec_item_dp_abc as class,
			'' as sub_class,
			CASE WHEN ps.stock_on_hand > 0 AND (cast(ps.stock_on_hand as int) > cast(ps.gross_requirement as int)) THEN 'Available'
			WHEN ps.stock_on_hand > 0 AND (cast(ps.stock_on_hand as int) < cast(ps.gross_requirement as int)) THEN 'Shortage'
			WHEN ps.stock_on_hand <= 0 THEN 'Out of Stock'
			END as supply_level,
			cast(ps.stock_on_hand as decimal(12,4))as stock_on_hand_projected, /* Increased the decimal value from 10 to 12  */
			cast(ps.safety_stock as decimal(12,4)) as projected_safety_stock,
			cast(ps.stock_coverage_no_of_weeks_for_soh as decimal(12,4)) as stock_coverage,
			cast(ps.purchase_order as decimal(12,4)) as count_of_purchase_order,
			cast(ps.goods_in_transit as decimal(12,4)) as inventory_in_transit,
			cast(ps.planned_receipt as decimal(12,4)) as planned_receipt,
			cast(ps.planned_available as decimal(12,4)) as planned_available,
			(cast(ps.gross_requirement as float) - cast(ps.stock_on_hand as float)) as shortage_quantity,
			null as shortage_time_duration,
			ps.out_of_stock_flag as oos_instance,
			'' as projected_service_level,
			cast(ps.gross_requirement as int) as demand_forecast,
			fmcode.custrecord_ec_mf_registered_code as sku_formulation,
			cast((item.[averagecost] * ps.[goods_in_transit])as decimal(12,4)) as stock_in_transit_value,
			cast((item.[averagecost] * ps.[stock_on_hand]) as float) as stock_on_hand_value,
			ps.week as planned_week,
			(item.[averagecost] * (cast(ps.gross_requirement as float) - cast(ps.stock_on_hand as float))) as shortage_value,
			getDate() AS md_record_written_timestamp,
			@pipelineid AS md_record_written_pipeline_id,
			@jobid AS md_transformation_job_id
			FROM
			std.fm_product_supply_alerts ps
			LEFT JOIN std.netsuite_item item
			ON ps.sku_code = item.itemid
			LEFT JOIN std.netsuite_location_combined lctn on
			ps.location_code = lctn.location_code
			LEFT JOIN std.date_dim dimdate on
			-- removed as part of UAT fix --ps.startdateofweek = FORMAT(dimdate.incr_date,'dd/MM/yyyy')
			ps.forecast_period = FORMAT(dimdate.incr_date,'dd/MM/yyyy')
			--LEFT JOIN std.netsuite_product_life_cycle plc ON ps.sku_code = plc.item
			LEFT JOIN std.netsuite_item_category ic ON item.custitem_ec_item_mk_category = ic.id
			LEFT JOIN std.netsuite_formulationcode fmcode ON item.custitem_ec_formu_currentformulation = fmcode.id
			--LEFT JOIN std.netsuite_regionlist rgnlst ON lctn.custrecord_ec_reporting_region = rgnlst.id
			--LEFT JOIN std.netsuite_subsidiary_combined nsc ON cast(nsc.sbs_no AS INT)=cast(lctn.subsidiary AS INT) WHERE lctn.parent is null
			LEFT JOIN [std].[netsuite_subsidiary] nssub on lctn.subsidiary = nssub.id
			LEFT JOIN [std].[subsidiary_x] subx on CAST(SUBSTRING(ps.location_code, 1, 2) as int) = subx.sbs_no
			LEFT JOIN [std].[netsuite_product_life_cycle] plc ON ps.[sku_code] = plc.[item] AND plc.item=item.itemid AND plc.country_code=subx.sbs_code_short
			WHERE lctn.parent is null and ps.location_code not like '[A-Z]%'
			OPTION (LABEL = 'AADPCONSPRODUCTSPLYCHNGEALERTS'); 

			UPDATE STATISTICS [cons_supply_chain].[product_supply_change_alerts];




				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPCONSPRODUCTSPLYCHNGEALERTS'

			EXEC meta_ctl.sp_row_count @jobid
				,@step_number
				,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_supply_chain.product_supply_change_alerts;
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM cons_supply_chain.product_supply_change_alerts WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_supply_chain.sp_cons_product_supply_change_alerts' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
