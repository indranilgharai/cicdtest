/****** Modified: Added category and subcategory logic to hardcode products in bundle lookup table as 'Non Sale' items    Script Date: 10/10/2023 6:00:00 PM  Modified By: Patrick Lacerna ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_ref_product_x_load] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			IF EXISTS (
				SELECT TOP 1 *
				FROM stage.dwh_product_x
				)
			BEGIN
				TRUNCATE TABLE std.product_x;
				PRINT 'INSIDE LOOP'

			INSERT INTO std.product_x
			SELECT
				cast([dcs_code] as [varchar](100)) dcs_code ,
				cast([dcs_name] as [varchar](100)) dcs_name ,
				cast([description1] as [varchar](100)) description1 ,
				cast([description2] as [varchar](100)) description2 ,
				cast([merge_code] as [varchar](100)) merge_code ,
				cast([merge_code_desc] as [varchar](100)) merge_code_desc ,
				cast([active] as [varchar](10)) active,
				-- Set category = 'Non Sale' for bundle items
				CASE WHEN ([buns].[bundle_code] is not null AND [prodx].[category] = 'Kits' AND [prodx].[sub_category] = 'Mixed Content Kit') 
					THEN 'Non Sale'
				ELSE cast([prodx].[category] as [varchar](100)) END AS category,
				-- Set sub_category = 'Bundle' for bundle items
				CASE WHEN ([buns].[bundle_code] is not null AND [prodx].[category] = 'Kits' AND [prodx].[sub_category] = 'Mixed Content Kit') 
					THEN 'Bundle'
				ELSE cast([prodx].[sub_category] as [varchar](100)) END AS sub_category,
				cast([kit_category] as [varchar](100)) kit_category ,
				cast([size] as [varchar](100)) size ,
				cast([discontinuation_date] as [varchar](100)) discontinuation_date ,
				cast([pack_size] as [int]) pack_size,
				cast([EAN] as [varchar](100)) EAN ,
				cast([sub_specific] as [varchar](100)) sub_specific ,
				cast([write_off] as [varchar](100)) write_off ,
				cast([ss_only] as [varchar](100)) ss_only ,
				cast([keyI1] as [varchar](100)) keyI1 ,
				cast([keyI2] as [varchar](100)) keyI2 ,
				cast([keyI3] as [varchar](100)) keyI3 ,
				cast([DG] as [varchar](10)) DG,
				cast([pricelist_category] as [varchar](100)) pricelist_category ,
				cast([pricelist_sub_category] as [varchar](100)) pricelist_sub_category ,
				cast([base_sku] as [varchar](100)) base_sku ,
				cast([product_type_cat] as [varchar](100)) product_type_cat ,
				cast([product_type_sub_cat] as [varchar](100)) product_type_sub_cat ,
				cast([status] as [varchar](100)) status ,
				cast([item_class] as [varchar](100)) item_class ,
				cast([item_sub_class] as [varchar](100)) item_sub_class ,
				cast([last_updated_date] as [datetime]) last_updated_datedatetime ,
				cast([netsuite_id] as [varchar](100)) netsuite_id ,
				cast([is_lot_numbered] as [varchar](10)) is_lot_numbered,
				cast([tmall_tax_code] as [varchar](100)) tmall_tax_code ,
				cast([barcode_type] as [varchar](100)) barcode_type ,
				cast([item_form] as [varchar](100)) item_form ,
				cast([short_description] as [varchar](100)) short_description ,
				cast([display_name] as [varchar](100)) display_name ,
				cast([dg_class] as [varchar](100)) dg_class ,
				cast([dg_packing_group] as [varchar](100)) dg_packing_group ,
				cast([dg_un_number] as [varchar](100)) dg_un_number ,
				cast([container] as [varchar](100)) container ,
				cast([full_carton_weight] as [float]) full_carton_weight ,
				cast([inner_carton] as [int]) inner_carton ,
				cast([item_gross_weight] as [float]) item_gross_weight ,
				cast([item_net_weight] as [float]) item_net_weight ,
				cast([item_unit] as [varchar](100)) item_unit ,
				cast([outer_carton] as [int]) outer_carton ,
				cast([packed_unit_volume] as [float]) packed_unit_volume,
				cast([packed_unit_weight] as [float]) packed_unit_weight,
				cast([product_depth] as [float]) product_depth,
				cast([product_height] as [float]) product_height,
				cast([product_width] as [float]) product_width,
				cast([unit] as [int]) unit,
				cast([export_pallet] as [int]) export_pallet,
				cast([fda_code] as [varchar](100)) fda_code ,
				cast([hs_code] as [varchar](100)) hs_code ,
				cast([country_of_origin] as [varchar](100)) country_of_origin ,
				cast([standard_pallet] as [int]) standard_pallet,
				cast([voc_percentage] as [float]) voc_percentage ,
				cast([synced_from_ns] as [varchar](10)) synced_from_ns ,
				cast([commodity_code] as [varchar](100)) commodity_code ,
				cast([shipper_width] as [float]) shipper_width,
				cast([shipper_height] as [float]) shipper_height ,
				cast([shipper_depth] as [float]) shipper_depth,
				cast([tax_schedule] as [varchar](100)) tax_schedule ,
				cast([send_to_wms] as [varchar](10)) send_to_wms,
				cast([netsuite_updated_date] as [varchar](100)) netsuite_updated_date ,
				getDate() AS md_record_written_timestamp,
				@pipelineid AS md_record_written_pipeline_id,
				@jobid AS md_transformation_job_id,
				'DWH' AS md_source_system
			FROM stage.dwh_product_x prodx
			LEFT JOIN (SELECT DISTINCT bundle_code FROM std.cegid_bundles) buns on prodx.description1 = buns.bundle_code

			OPTION (LABEL = 'AADPPRODX');

			UPDATE STATISTICS [std].[product_x];
			
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPPRODX'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

TRUNCATE TABLE stage.dwh_product_x;
				PRINT 'TRUNCATED STAGE'
			END
			ELSE
			BEGIN
				PRINT 'Stage is Empty'
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.product_x;
			SELECT @onlydate = CAST(@newrec AS DATE);

			
			DELETE	FROM std.product_x	WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_ref_product_x_load' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
