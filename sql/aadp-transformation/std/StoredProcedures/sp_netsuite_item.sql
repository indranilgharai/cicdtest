/****** Object:  StoredProcedure [std].[sp_netsuite_item]    Script Date: 4/22/2022 3:49:09 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_item] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			DECLARE @max_ingestion_date [varchar](500)
			select @max_ingestion_date=max(CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )) from [std].[netsuite_item]; 
			
			
			insert into [std].[netsuite_item]
			select [alternatedemandsourceitem],
			[amortizationperiod],
			[amortizationtemplate],
			[assetaccount],
			[atpmethod],
			[averagecost],
			[backwardconsumptiondays],
			[billexchratevarianceacct],
			[billpricevarianceacct],
			[billqtyvarianceacct],
			[buildentireassembly],
			[class],
			[copydescription],
			[cost],
			[costcategory],
			[costestimate],
			[costestimatetype],
			[costingmethod],
			[costingmethoddisplay],
			[countryofmanufacture],
			[createddate],
			[createexpenseplanson],
			[cseg_ec_sales_src],
			[custitem1],
			[custitem_4601_defaultwitaxcode],
			[custitem_clu_il1],
			[custitem_clu_il2],
			[custitem_clu_il3],
			[custitem_clu_il4],
			[custitem_code_of_supply],
			[custitem_commodity_code],
			[custitem_cseg_aesop_eu],
			[custitem_ec_additional_information],
			[custitem_ec_barcode_type],
			[custitem_ec_boiling_point],
			[custitem_ec_bom_level],
			[custitem_ec_calculated_cost],
			[custitem_ec_cegid_closed],
			[custitem_ec_cegid_no_replenishment],
			[custitem_ec_cegid_non_sellable],
			[custitem_ec_cegid_plc_available],
			[custitem_ec_cegid_plc_hash],
			[custitem_ec_cegid_plc_not_available],
			[custitem_ec_dg_hazchem_code],
			[custitem_ec_dg_packing_instruction],
			[custitem_ec_dg_proper_shipping_name],
			[custitem_ec_dg_technical_name_nos],
			[custitem_ec_english_registered],
			[custitem_ec_exclude_from_3pl_rec],
			[custitem_ec_flash_point],
			[custitem_ec_fm_special_pricing],
			[custitem_ec_formu_currentformulation],
			[custitem_ec_hybris_benefit],
			[custitem_ec_hybris_form],
			[custitem_ec_hybris_last_modified],
			[custitem_ec_hybris_product_category],
			[custitem_ec_hybris_product_sub_cat],
			[custitem_ec_hybris_range],
			[custitem_ec_hybris_skin_type],
			[custitem_ec_insurancestock],
			[custitem_ec_item_class],
			[custitem_ec_item_cofa_test_odour],
			[custitem_ec_item_cosmetic_number],
			[custitem_ec_item_dp_abc],
			[custitem_ec_item_dp_label_direction],
			[custitem_ec_item_dp_labelsize],
			[custitem_ec_item_dp_leadtimevariancep],
			[custitem_ec_item_eoblend_cofa_test_app],
			[custitem_ec_item_exp_pallet],
			[custitem_ec_item_fda_code],
			[custitem_ec_item_hybris_cpsf],
			[custitem_ec_item_hybris_deaflag],
			SUBSTRING([custitem_ec_item_hybris_sellableonline],1,500),
			[custitem_ec_item_hybris_size],
			[custitem_ec_item_manufacture_labelcode],
			[custitem_ec_item_master_loc],
			[custitem_ec_item_merge_code],
			[custitem_ec_item_merge_code_backup],
			[custitem_ec_item_mf_prodbatchsize1],
			[custitem_ec_item_mf_prodbatchsize2],
			[custitem_ec_item_mf_prodbatchsize3],
			[custitem_ec_item_mk_category],
			[custitem_ec_item_mk_item_class],
			[custitem_ec_item_mk_item_sub_class],
			[custitem_ec_item_mk_sub_category],
			[custitem_ec_item_pk_compliance_check],
			[custitem_ec_item_pk_energy_use],
			[custitem_ec_item_pk_origin],
			[custitem_ec_item_pk_recycled_content],
			[custitem_ec_item_pk_water_use],
			[custitem_ec_item_product_status],
			[custitem_ec_item_productvarianttype],
			[custitem_ec_item_ps_container],
			[custitem_ec_item_ps_dangerousgood],
			[custitem_ec_item_ps_fullboxweight],
			[custitem_ec_item_ps_grossweight],
			[custitem_ec_item_ps_hscode],
			[custitem_ec_item_ps_hscode2],
			[custitem_ec_item_ps_inner],
			[custitem_ec_item_ps_netweight],
			[custitem_ec_item_ps_origin],
			[custitem_ec_item_ps_outer],
			[custitem_ec_item_ps_packedunitvolume],
			[custitem_ec_item_ps_packedunitweight],
			[custitem_ec_item_ps_pallet],
			[custitem_ec_item_ps_productheight],
			[custitem_ec_item_ps_productlength],
			[custitem_ec_item_ps_productwidthdiamet],
			[custitem_ec_item_ps_retailpacksize],
			[custitem_ec_item_ps_unit],
			[custitem_ec_item_rm_inciname],
			[custitem_ec_item_rm_rminternalname],
			[custitem_ec_item_shipper_item],
			[custitem_ec_item_size],
			[custitem_ec_item_stock_description],
			[custitem_ec_item_sync_to_3pl],
			[custitem_ec_item_test_cofa_colour],
			[custitem_ec_item_un_number],
			[custitem_ec_item_unit],
			[custitem_ec_item_use2nd_coef],
			[custitem_ec_item_ws_approval_date],
			[custitem_ec_item_ws_approval_number],
			[custitem_ec_mf_bom_item],
			[custitem_ec_mf_work_order_item],
			[custitem_ec_natura_code],
			[custitem_ec_not_forecasted],
			[custitem_ec_packaging_family],
			[custitem_ec_packing_group],
			[custitem_ec_packing_shipperconfig],
			[custitem_ec_packing_spec_code],
			[custitem_ec_parent_code],
			[custitem_ec_plc_cegid_code],
			SUBSTRING([custitem_ec_pricing_group],1,500),
			[custitem_ec_product_expiry],
			[custitem_ec_product_sales_category],
			[custitem_ec_product_type_category],
			[custitem_ec_product_type_sub_category],
			[custitem_ec_record_status],
			[custitem_ec_refill_qty],
			[custitem_ec_rp_alternate_item],
			[custitem_ec_sample_pack_size],
			[custitem_ec_send_to_hybris],
			[custitem_ec_short_description],
			[custitem_ec_supplied],
			[custitem_ec_sync_to_fm_dp],
			[custitem_ec_sync_to_fm_drp],
			[custitem_ec_tmall_tax_code],
			[custitem_il_ei_is_abbuono],
			[custitem_il_ei_is_premio],
			[custitem_il_ei_is_stamp],
			[custitem_il_ei_spesa_accessoria],
			[custitem_itr_supplementary_unit],
			[custitem_itr_supplementary_unit_abbrev],
			[custitem_nature_of_transaction_codes],
			[custitem_prompt_payment_discount_item],
			[custitem_reg_required],
			[custitem_sps_item_synch],
			[custitem_type_of_goods],
			[custitem_un_number],
			[custitem_voc_perc],
			[custreturnvarianceaccount],
			[deferralaccount],
			[demandmodifier],
			[demandsource],
			[demandtimefence],
			[department],
			[description],
			[displayname],
			[distributioncategory],
			[distributionnetwork],
			[dontshowprice],
			[dropshipexpenseaccount],
			[enforceminqtyinternally],
			[excludefromsitemap],
			[expenseaccount],
			[expenseamortizationrule],
			[externalid],
			[featureddescription],
			[fixedlotsize],
			[forwardconsumptiondays],
			[froogleproductfeed],
			[fullname],
			[fxcost],
			[gainlossaccount],
			[generateaccruals],
			[handlingcost],
			[id],
			[includechildren],
			[incomeaccount],
			[intercoexpenseaccount],
			[intercoincomeaccount],
			[isdonationitem],
			[isdropshipitem],
			[isfulfillable],
			[isinactive],
			[islotitem],
			[isonline],
			[isphantom],
			[isserialitem],
			[isspecialorderitem],
			[isspecialworkorderitem],
			[itemid],
			[itemtype],
			[lastmodifieddate],
			[lastpurchaseprice],
			[location],
			[manufacturer],
			[matchbilltoreceipt],
			[matrixoptioncustitem1],
			[maxdonationamount],
			[maximumquantity],
			[metataghtml],
			[minimumquantity],
			[mpn],
			[nextagcategory],
			[nextagproductfeed],
			[nopricemessage],
			[outofstockbehavior],
			[outofstockmessage],
			[overallquantitypricingtype],
			[overheadtype],
			[pagetitle],
			[parent],
			[periodiclotsizedays],
			[periodiclotsizetype],
			[preferredlocation],
			[pricinggroup],
			[printitems],
			[prodpricevarianceacct],
			[prodqtyvarianceacct],
			[purchasedescription],
			[purchaseorderamount],
			[purchaseorderquantity],
			[purchaseorderquantitydiff],
			[purchasepricevarianceacct],
			[quantitypricingschedule],
			[receiptamount],
			[receiptquantity],
			[receiptquantitydiff],
			[relateditemsdescription],
			[rescheduleindays],
			[rescheduleoutdays],
			[residual],
			[roundupascomponent],
			[scrapacct],
			[searchkeywords],
			[seasonaldemand],
			[shipindividually],
			[shippackage],
			[shippingcost],
			[shoppingdotcomcategory],
			[shoppingproductfeed],
			[shopzillacategoryid],
			[shopzillaproductfeed],
			[showdefaultdonationamount],
			[sitemappriority],
			[stockdescription],
			[storedescription],
			SUBSTRING([storedetaileddescription],1,500),
			[storedisplayimage],
			[storedisplayname],
			[storedisplaythumbnail],
			[subsidiary],
			[subtype],
			[supplylotsizingmethod],
			[supplyreplenishmentmethod],
			[supplytimefence],
			[supplytype],
			[totalquantityonhand],
			[totalvalue],
			[transferprice],
			[unbuildvarianceaccount],
			[upccode],
			[urlcomponent],
			[usebins],
			[usemarginalrates],
			[vendorname],
			[vendreturnvarianceaccount],
			[weight],
			[weightunit],
			[wipacct],
			[wipvarianceacct],
			[yahooproductfeed],
			getDate() as md_record_written_timestamp,
			@pipelineid as md_record_written_pipeline_id,
			@jobid as md_transformation_job_id,
			'Netsuite'
			from stage.netsuite_item
			where CAST(convert(datetime,md_record_ingestion_timestamp,103) as datetime )>coalesce(@max_ingestion_date,CAST(convert(datetime,'01-01-1980 00:00:00',103) as datetime ))
			OPTION (LABEL = 'AADPSTDITEM');

			UPDATE STATISTICS [std].[netsuite_item];
			
			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDITEM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.netsuite_item;
			
			DELETE FROM std.netsuite_item WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:std.netsuite_item'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_netsuite_item' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
GO


