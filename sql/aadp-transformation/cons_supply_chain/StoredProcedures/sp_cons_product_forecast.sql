-- ## SP for load of Consumption table : PRODUCT_FORECAST ##
-- Modified Script [16/08/2022]:added new forecast flags that identifies latest records until 1, 2 and 3 months ago from today
-- Modified Script [24/08/2022]:modified forecast flags that identifies latest records until 1, 3 and 6 months ago from today
-- Modified Script [30/09/2022]:added new channel codes so they align with channel codes in Netsuite - Sonia Lin
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_supply_chain].[sp_cons_product_forecast] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			TRUNCATE TABLE [cons_supply_chain].[product_forecast];
			
			INSERT INTO [cons_supply_chain].[product_forecast]
			SELECT DISTINCT
			  pf.[sku_code] as sku,
			  item.description as sku_description,
			  pf.[forecast_date] as forecast_date,
			  pf.[start_date] as forecast_period,
			  DATEPART(MM,pf.[start_date]) as forecast_month_number,
			  DATEPART(YY,pf.[start_date]) as forecast_period_year,
			  pf.[latest_forecast_flag] as latest_forecast_flag,
			  subx.sbs_region as region,
			  --nssub.name as subsidiary,
			  subx.sbs_name as subsidiary,
			  pf.[location_code] as location,
			  pf.[channel_code] as channel_code,
			  --class.name as class,
			  -- New channel codes added to align with channel codes in Netsuite
			  CASE
				WHEN pf.[channel_code] = 'CL010' THEN 'Retail : Department Store'
				WHEN pf.[channel_code] = 'CL020' THEN 'Aesop.com'
				WHEN pf.[channel_code] = 'CL021' THEN 'Third Party Digital'
				WHEN pf.[channel_code] = 'CL030' THEN 'Wholesale : Distributors'
				WHEN pf.[channel_code] = 'CL040' THEN 'Intercompany'
				WHEN pf.[channel_code] = 'CL051' THEN 'Office expenses : Head Office Expenses'
				WHEN pf.[channel_code] = 'CL052' THEN 'Office expenses : Local Office Expenses'
				WHEN pf.[channel_code] = 'CL053' THEN 'Office expenses : Regional Office Expenses'
				WHEN pf.[channel_code] = 'CL060' THEN 'Retail : Signature store'
				WHEN pf.[channel_code] = 'CL070' THEN 'Wholesale : General-stocklists'
				WHEN pf.[channel_code] = 'CL071' THEN 'Wholesale : Department Store'
				WHEN pf.[channel_code] = 'CL072' THEN 'Wholesale : Online Reseller'
			  END as class,
			  iic.product_type as Product_type_category,
			  iic.name as item_category,
			  sc.name as item_sub_category,
			  plc.[plc_status] as product_life_cycle,
			  pf.[project_demand_units] as project_demand_units,
			  pf.[cleared_actual_units] as cleared_actual_units,
			  item.[averagecost] * pf.[project_demand_units] as projected_demand_value_AUD,
			  item.[averagecost] * pf.[cleared_actual_units] as actual_demand_value_AUD,
			  case when subx.sbs_code_short='AU' then (item.[averagecost] * pf.[project_demand_units]) else ((item.[averagecost] * pf.[project_demand_units])/exrate.ex_rate) end as projected_demand_value_local,
			  case when subx.sbs_code_short='AU' then (item.[averagecost] * pf.[cleared_actual_units]) else ((item.[averagecost] * pf.[cleared_actual_units])/exrate.ex_rate) end as actual_demand_value_local,
			  case WHEN IsNULL(pf.project_demand_units, '0') > 0 AND IsNULL(pf.cleared_actual_units, '0') > 0
				THEN (100 - CAST(abs(pf.cleared_actual_units - pf.project_demand_units) as float) /pf.cleared_actual_units * 100) 
				ELSE '0' END as percentage_accuracy,
			  DATEDIFF(month,pf.[start_date], pf.[forecast_date]) as percentage_bias,
			  NULL as accuracy_target,			
			  getdate() as [md_record_written_timestamp],
			  @pipelineid as [md_record_written_pipeline_id],
			  @jobid as [md_transformation_job_id],
			  -- added month-1,month-3,month-6 forecast flags
			  pf.[m_1_forecast_flag] as m_1_forecast_flag,
			  pf.[m_3_forecast_flag] as m_3_forecast_flag,
			  pf.[m_6_forecast_flag] as m_6_forecast_flag
			FROM [std].[fm_product_forecast]  pf
			--LEFT JOIN [std].[netsuite_product_life_cycle] plc ON pf.[sku_code] = plc.[item]
			
			LEFT JOIN [std].[netsuite_item] item ON pf.[sku_code] = item.[itemid]
			
			LEFT JOIN [std].[netsuite_item_category] ic ON item.custitem_ec_item_mk_category = ic.id
			
			LEFT JOIN [std].[netsuite_class] class ON pf.channel_code = class.custrecord_ec_class_code
			
			LEFT JOIN (SELECT item.itemID,ic.name,ptc.name as product_type FROM [std].[netsuite_item] item 
						LEFT JOIN [std].[netsuite_item_category] ic ON item.[custitem_ec_item_mk_category] = ic.id
						LEFT JOIN  [std].[netsuite_product_type_category] ptc ON item.[custitem_ec_product_type_category] = ptc.id) iic ON  pf.[sku_code] = iic.itemID
			--item_sub_category--
			LEFT JOIN [std].[netsuite_item_subcategory] sc ON item.custitem_ec_item_mk_sub_category = sc.id
			LEFT JOIN (SELECT subsidiary,name,custrecord_ec_loc_report_city,[custrecord_ec_location_code] FROM [std].[netsuite_location] WHERE PARENT is null) nlo on pf.location_code = nlo.[custrecord_ec_location_code]
			--projected_demand_value_local,actual_demand_value_local--
			LEFT JOIN (select * from (select sbs_no,ex_rate,year,month_no,row_number() over(partition by sbs_no order by year desc,month_no desc) rwno from [std].[exchange_rate_x] )a where rwno=1 ) exrate 
					ON cast(nlo.subsidiary AS INT) = cast(exrate.sbs_no AS INT) 
			LEFT JOIN [std].[netsuite_subsidiary] nssub on nlo.subsidiary = nssub.id
			LEFT JOIN [std].[subsidiary_x] subx on CAST(SUBSTRING(pf.location_code, 1, 2) as int) = subx.sbs_no
			LEFT JOIN [std].[netsuite_product_life_cycle] plc ON pf.[sku_code] = plc.[item] AND plc.item=item.itemid AND plc.country_code=subx.sbs_code_short
			where pf.location_code not like '[A-Z]%'
			OPTION (LABEL = 'AADPCONSPRODUCTFORECAST');

		    UPDATE STATISTICS [cons_supply_chain].[product_forecast];
			
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)
			SET @label = 'AADPCONSPRODUCTFORECAST'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM cons_supply_chain.product_forecast;
			
			DELETE FROM cons_supply_chain.product_forecast WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Consumption table:cons_supply_chain.product_forecast'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_supply_chain.sp_cons_product_forecast' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END