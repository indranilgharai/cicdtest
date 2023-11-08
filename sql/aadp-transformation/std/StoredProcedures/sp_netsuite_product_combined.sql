-- ## SP for load of Standardized table : PRODUCT_FORECAST ##
-- Modified Script [02/11/2022] : added condition to avoid the error for pricing_group column if the length of the value is more than expected length
-- Modified Script [15/12/2022] : updated column storedetaileddescription to null to avoid data overflow issue
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_product_combined] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			IF OBJECT_ID('tempdb..#netsuite_item_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_item_temp
			END
			
			-----------------------temporary table to pick last ingested data ---------------------------
			
			create table #netsuite_item_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select * from(
			select distinct *,rank() over(partition by itemid order by [md_record_ingestion_timestamp] desc,lastmodifieddate desc) as dupcnt 
			from stage.netsuite_item)a where dupcnt=1 ;
			
			Insert into [std].[netsuite_product_combined]
			SELECT DISTINCT
			coalesce(it.itemid,prod.description1) as product_id
			,prod.dcs_code as dcs_code
			,prod.dcs_name as dcs_name
			,prod.description1 as description1
			,prod.description2 as description2
			,prod.merge_code as merge_code
			,prod.merge_code_desc as merge_code_desc
			,prod.active as active
			,prod.category as category
			,prod.sub_category as sub_category
			,prod.kit_category as kit_category
			,prod.size as size
			,prod.discontinuation_date as discontinuation_date
			,prod.pack_size as pack_size
			,prod.EAN as EAN
			,prod.sub_specific as sub_specific
			,prod.write_off as write_off
			,prod.ss_only as ss_only
			,prod.keyI1 as keyI1
			,prod.keyI2 as keyI2
			,prod.keyI3 as keyI3
			,prod.DG as DG
			,prod.pricelist_category as pricelist_category
			,prod.pricelist_sub_category as pricelist_sub_category
			,prod.base_sku as base_sku
			,prod.product_type_cat as product_type_cat
			,prod.product_type_sub_cat as product_type_sub_cat
			,prod.status as status
			,prod.item_class as item_class--
			,prod.item_sub_class as item_sub_class
			,prod.last_updated_date as last_updated_date
			,prod.netsuite_id as netsuite_id
			,prod.is_lot_numbered as is_lot_numbered
			,prod.tmall_tax_code as tmall_tax_code--
			,prod.barcode_type as barcode_type--
			,prod.item_form as item_form
			,prod.short_description as short_description
			,prod.display_name as display_name
			,prod.dg_class as dg_class
			,prod.dg_packing_group as dg_packing_group
			,prod.dg_un_number as dg_un_number
			,prod.container as container
			,prod.full_carton_weight as full_carton_weight
			,prod.inner_carton as inner_carton
			,prod.item_gross_weight as item_gross_weight
			,prod.item_net_weight as item_net_weight
			,prod.item_unit as item_unit--
			,prod.outer_carton as outer_carton
			,prod.packed_unit_volume as packed_unit_volume
			,prod.packed_unit_weight as packed_unit_weight
			,prod.product_depth as product_depth
			,prod.product_height as product_height
			,prod.product_width as product_width
			,prod.unit as unit
			,prod.export_pallet as export_pallet
			,prod.fda_code as fda_code
			,prod.hs_code as hs_code
			,prod.country_of_origin as country_of_origin
			,prod.standard_pallet as standard_pallet
			,prod.voc_percentage as voc_percentage
			,prod.synced_from_ns as synced_from_ns
			,prod.commodity_code as commodity_code
			,prod.shipper_width as shipper_width
			,prod.shipper_height as shipper_height
			,prod.shipper_depth as shipper_depth
			,prod.tax_schedule as tax_schedule
			,prod.send_to_wms as send_to_wms
			,prod.netsuite_updated_date as netsuite_updated_date
			
			,it.alternatedemandsourceitem as alternatedemandsourceitem
			,it.amortizationperiod as amortizationperiod
			,it.amortizationtemplate as amortizationtemplate
			,it.assetaccount as assetaccount
			,it.atpmethod  as atpmethod
			,it.averagecost as averagecost
			,it.backwardconsumptiondays as backwardconsumptiondays
			,it.billexchratevarianceacct as billexchratevarianceacct
			,it.billpricevarianceacct as billpricevarianceacct
			,it.billqtyvarianceacct as billqtyvarianceacct
			,it.buildentireassembly as buildentireassembly
			,it.class as class
			,it.copydescription as copydescription
			,it.cost as cost
			,it.costcategory as costcategory
			,it.costestimate as costestimate
			,it.costestimatetype as costestimatetype
			,it.costingmethod as costingmethod
			,it.costingmethoddisplay as costingmethoddisplay
			,it.countryofmanufacture as countryofmanufacture
			,it.createddate as createddate
			,it.createexpenseplanson as createexpenseplanson
			,it.cseg_ec_sales_src as cseg_sales_src
			,it.custitem1 as custitem1
			,it.custitem_4601_defaultwitaxcode as [4601_defaultwitaxcode]
			,it.custitem_clu_il1  as clu_il1
			,it.custitem_clu_il2  as clu_il2
			,it.custitem_clu_il3  as clu_il3
			,it.custitem_clu_il4  as clu_il4
			,it.custitem_code_of_supply as code_of_supply
			,it.custitem_commodity_code as ns_commodity_code
			,it.custitem_cseg_aesop_eu as cseg_aesop_eu
			,it.custitem_ec_additional_information as additional_information
			,it.custitem_ec_barcode_type as ns_barcode_type
			,it.custitem_ec_boiling_point as boiling_point
			,it.custitem_ec_bom_level as bom_level
			,it.custitem_ec_calculated_cost as calculated_cost
			,it.custitem_ec_cegid_closed as cegid_closed
			,it.custitem_ec_cegid_no_replenishment as cegid_no_replenishment
			,it.custitem_ec_cegid_non_sellable as cegid_non_sellable
			,it.custitem_ec_cegid_plc_available as cegid_plc_available
			,it.custitem_ec_cegid_plc_hash as cegid_plc_hash
			,it.custitem_ec_cegid_plc_not_available as cegid_plc_not_available
			,it.custitem_ec_dg_hazchem_code as dg_hazchem_code
			,it.custitem_ec_dg_packing_instruction as dg_packing_instruction
			,it.custitem_ec_dg_proper_shipping_name as dg_proper_shipping_name
			,it.custitem_ec_dg_technical_name_nos as dg_technical_name_nos
			,it.custitem_ec_english_registered as english_registered
			,it.custitem_ec_exclude_from_3pl_rec as exclude_from_3pl_rec 
			,it.custitem_ec_flash_point as flash_point
			,it.custitem_ec_fm_special_pricing as fm_special_pricing
			,it.custitem_ec_formu_currentformulation as formu_currentformulation
			,it.custitem_ec_hybris_benefit as hybris_benefit
			,it.custitem_ec_hybris_form as hybris_form
			,it.custitem_ec_hybris_last_modified as hybris_last_modified
			,it.custitem_ec_hybris_product_category as hybris_product_category
			,it.custitem_ec_hybris_product_sub_cat as hybris_product_sub_cat
			,it.custitem_ec_hybris_range as hybris_range
			,it.custitem_ec_hybris_skin_type as hybris_skin_type
			,it.custitem_ec_insurancestock as insurancestock
			,it.custitem_ec_item_class as ns_item_class
			,it.custitem_ec_item_cofa_test_odour as item_cofa_test_odour
			,it.custitem_ec_item_dp_abc as item_dp_abc
			,it.custitem_ec_item_dp_label_direction as item_dp_label_direction
			,it.custitem_ec_item_dp_labelsize as item_dp_labelsize
			,it.custitem_ec_item_dp_leadtimevariancep as item_dp_leadtimevariancep
			,it.custitem_ec_item_eoblend_cofa_test_app as item_eoblend_cofa_test_app
			,it.custitem_ec_item_exp_pallet as item_exp_pallet
			,it.custitem_ec_item_fda_code as item_fda_code
			,it.custitem_ec_item_hybris_cpsf as item_hybris_cpsf
			,it.custitem_ec_item_hybris_deaflag as item_hybris_deaflag
			,it.custitem_ec_item_hybris_sellableonline as item_hybris_sellableonline
			,it.custitem_ec_item_hybris_size as item_hybris_size
			,it.custitem_ec_item_manufacture_labelcode as item_manufacture_labelcode
			,it.custitem_ec_item_master_loc as item_master_loc 
			,it.custitem_ec_item_merge_code as item_merge_code
			,it.custitem_ec_item_merge_code_backup	 as item_merge_code_backup
			,it.custitem_ec_item_mf_prodbatchsize1	 as item_mf_prodbatchsize1
			,it.custitem_ec_item_mf_prodbatchsize2	 as item_mf_prodbatchsize2
			,it.custitem_ec_item_mf_prodbatchsize3	 as item_mf_prodbatchsize3
			,ic.[name] as item_mk_category
			,it.custitem_ec_item_mk_item_class as item_mk_item_class
			,it.custitem_ec_item_mk_item_sub_class as item_mk_item_sub_class
			,isc.[name] as item_mk_sub_category
			,it.custitem_ec_item_pk_compliance_check as item_pk_compliance_check
			,it.custitem_ec_item_pk_energy_use as item_pk_energy_use
			,it.custitem_ec_item_pk_origin as item_pk_origin
			,it.custitem_ec_item_pk_recycled_content as item_pk_recycled_content
			,it.custitem_ec_item_pk_water_use as item_pk_water_use
			,it.custitem_ec_item_product_status as item_product_status
			,it.custitem_ec_item_productvarianttype as item_productvarianttype
			,it.custitem_ec_item_ps_container as item_ps_container
			,it.custitem_ec_item_ps_dangerousgood as item_ps_dangerousgood
			,it.custitem_ec_item_ps_fullboxweight as item_ps_fullboxweight
			,it.custitem_ec_item_ps_grossweight as item_ps_grossweight
			,it.custitem_ec_item_ps_hscode as item_ps_hscode
			,it.custitem_ec_item_ps_hscode2 as item_ps_hscode2
			,it.custitem_ec_item_ps_inner as item_ps_inner
			,it.custitem_ec_item_ps_netweight as item_ps_netweight
			,it.custitem_ec_item_ps_origin as item_ps_origin
			,it.custitem_ec_item_ps_outer as item_ps_outer 
			,it.custitem_ec_item_ps_packedunitvolume as item_ps_packedunitvolume
			,it.custitem_ec_item_ps_packedunitweight as item_ps_packedunitweight
			,it.custitem_ec_item_ps_pallet as item_ps_pallet
			,it.custitem_ec_item_ps_productheight as item_ps_productheight
			,it.custitem_ec_item_ps_productlength as item_ps_productlength
			,it.custitem_ec_item_ps_productwidthdiamet as item_ps_productwidthdiamet
			,it.custitem_ec_item_ps_retailpacksize as item_ps_retailpacksize
			,it.custitem_ec_item_ps_unit as item_ps_unit
			,it.custitem_ec_item_rm_inciname as item_rm_inciname
			,it.custitem_ec_item_rm_rminternalname as item_rm_rminternalname
			,it.custitem_ec_item_shipper_item as item_shipper_item
			,it.custitem_ec_item_size as item_size
			,it.custitem_ec_item_stock_description as item_stock_description
			,it.custitem_ec_item_sync_to_3pl as item_sync_to_3pl
			,it.custitem_ec_item_test_cofa_colour as item_test_cofa_colour
			,it.custitem_ec_item_un_number as item_un_number
			,it.custitem_ec_item_unit as ns_item_unit
			,it.custitem_ec_item_use2nd_coef as item_use2nd_coef
			,it.custitem_ec_item_ws_approval_date as item_ws_approval_date
			,it.custitem_ec_item_ws_approval_number as item_ws_approval_number 
			,it.custitem_ec_mf_bom_item as mf_bom_item
			,it.custitem_ec_mf_work_order_item as mf_work_order_item
			,it.custitem_ec_natura_code as natura_code
			,it.custitem_ec_not_forecasted as not_forecasted
			,it.custitem_ec_packaging_family as packaging_family
			,it.custitem_ec_packing_group as packing_group
			,it.custitem_ec_packing_shipperconfig as packing_shipperconfig
			,it.custitem_ec_packing_spec_code as packing_spcode
			,it.custitem_ec_parent_code as parent_code
			,it.custitem_ec_plc_cegid_code as plc_cegid_code
			--added condition to truncate the value in pricing_group column if the length of the value is more than 1200
			,
			case 
			when len(it.custitem_ec_pricing_group)>1200
			then left(it.custitem_ec_pricing_group, 1200) 
			else it.custitem_ec_pricing_group end as pricing_group
			,it.custitem_ec_product_expiry as product_expiry
			,it.custitem_ec_product_sales_category as product_sales_category
			,ptc.[name] as product_type_category
			,it.custitem_ec_product_type_sub_category as product_type_sub_category
			,it.custitem_ec_record_status as record_status
			,it.custitem_ec_refill_qty as refill_qty
			,it.custitem_ec_rp_alternate_item as rp_alternate_item
			,it.custitem_ec_sample_pack_size as sample_pack_size
			,it.custitem_ec_send_to_hybris as send_to_hybris
			,it.custitem_ec_short_description as ns_short_description
			,it.custitem_ec_supplied as supplied 
			,it.custitem_ec_sync_to_fm_dp as sync_to_fm_dp
			,it.custitem_ec_sync_to_fm_drp as sync_to_fm_drp
			,it.custitem_ec_tmall_tax_code as ns_tmall_tax_code
			,it.custitem_il_ei_is_abbuono as il_ei_is_abbuono
			,it.custitem_il_ei_is_premio as il_ei_is_premio
			,it.custitem_il_ei_is_stamp as il_ei_is_stamp
			,it.custitem_il_ei_spesa_accessoria as il_ei_spesa_accessoria
			,it.custitem_itr_supplementary_unit as itr_supplementary_unit
			,it.custitem_itr_supplementary_unit_abbrev as itr_supplementary_unit_abbrev
			,it.custitem_nature_of_transaction_codes as nature_of_transaction_codes
			,it.custitem_prompt_payment_discount_item as prompt_payment_discount_item
			,it.custitem_reg_required as reg_required
			,it.custitem_sps_item_synch as sps_item_synch
			,it.custitem_type_of_goods as type_of_goods
			,it.custitem_un_number as un_number
			,it.custitem_voc_perc as voc_perc
			,it.custreturnvarianceaccount as custreturnvarianceaccount
			,it.deferralaccount as deferralaccount
			,it.demandmodifier as demandmodifier
			,it.demandsource as demandsource
			,it.demandtimefence as demandtimefence
			,it.department as department
			,it.[description] as [description]
			,it.displayname as displayname
			,it.distributioncategory as distributioncategory
			,it.distributionnetwork as distributionnetwork
			,it.dontshowprice as dontshowprice
			,it.dropshipexpenseaccount as dropshipexpenseaccount
			,it.enforceminqtyinternally as enforceminqtyinternally
			,it.excludefromsitemap as excludefromsitemap
			,it.expenseaccount as expenseaccount
			,it.expenseamortizationrule as expenseamortizationrule
			,it.externalid as externalid
			,it.featureddescription as featureddescription
			,it.fixedlotsize as fixedlotsize
			,it.forwardconsumptiondays as forwardconsumptiondays
			,it.froogleproductfeed as froogleproductfeed
			,it.fullname as fullname
			,it.fxcost as fxcost
			,it.gainlossaccount as gainlossaccount
			,it.generateaccruals as generateaccruals
			,it.handlingcost as handlingcost
			,it.id as id
			,it.includechildren as includechildren
			,it.incomeaccount as incomeaccount
			,it.intercoexpenseaccount as intercoexpenseaccount
			,it.intercoincomeaccount as intercoincomeaccount
			,it.isdonationitem as isdonationitem
			,it.isdropshipitem as isdropshipitem
			,it.isfulfillable as isfulfillable
			,it.isinactive as isinactive
			,it.islotitem as islotitem
			,it.isonline as isonline
			,it.isphantom as isphantom
			,it.isserialitem as isserialitem
			,it.isspecialorderitem as isspecialorderitem
			,it.isspecialworkorderitem as isspecialworkorderitem
			,it.itemid as itemid
			,it.itemtype as itemtype
			,it.lastmodifieddate as lastmodifieddate
			,it.lastpurchaseprice as lastpurchaseprice
			,it.[location] as [location]
			,it.manufacturer as manufacturer
			,it.matchbilltoreceipt as matchbilltoreceipt
			,it.matrixoptioncustitem1 as matrixoptioncustitem1
			,it.maxdonationamount as maxdonationamount
			,it.maximumquantity as maximumquantity
			,it.metataghtml as metataghtml
			,it.minimumquantity as minimumquantity
			,it.mpn as mpn
			,it.nextagcategory as nextagcategory
			,it.nextagproductfeed as nextagproductfeed
			,it.nopricemessage as nopricemessage
			,it.outofstockbehavior as outofstockbehavior
			,it.outofstockmessage as outofstockmessage
			,it.overallquantitypricingtype as overallquantitypricingtype
			,it.overheadtype as overheadtype
			,it.pagetitle as pagetitle
			,it.parent as parent
			,it.periodiclotsizedays as periodiclotsizedays
			,it.periodiclotsizetype as periodiclotsizetype
			,it.preferredlocation as preferredlocation
			,it.pricinggroup as pricinggroup
			,it.printitems as printitems
			,it.prodpricevarianceacct as prodpricevarianceacct
			,it.prodqtyvarianceacct as prodqtyvarianceacct
			,it.purchasedescription as purchasedescription
			,it.purchaseorderamount as purchaseorderamount
			,it.purchaseorderquantity as purchaseorderquantity
			,it.purchaseorderquantitydiff as purchaseorderquantitydiff
			,it.purchasepricevarianceacct as purchasepricevarianceacct
			,it.quantitypricingschedule as quantitypricingschedule
			,it.receiptamount as receiptamount
			,it.receiptquantity as receiptquantity
			,it.receiptquantitydiff as receiptquantitydiff
			,it.relateditemsdescription as relateditemsdescription
			,it.rescheduleindays as rescheduleindays
			,it.rescheduleoutdays as rescheduleoutdays
			,it.residual as residual
			,it.roundupascomponent as roundupascomponent
			,it.scrapacct as scrapacct
			,it.searchkeywords as searchkeywords
			,it.seasonaldemand as seasonaldemand
			,it.shipindividually as shipindividually
			,it.shippackage as shippackage
			,it.shippingcost as shippingcost
			,it.shoppingdotcomcategory as shoppingdotcomcategory
			,it.shoppingproductfeed as shoppingproductfeed
			,it.shopzillacategoryid as shopzillacategoryid
			,it.shopzillaproductfeed as shopzillaproductfeed
			,it.showdefaultdonationamount as showdefaultdonationamount
			,it.sitemappriority as sitemappriority
			,it.stockdescription as stockdescription
			,it.storedescription as storedescription
			,null as storedetaileddescription
			,it.storedisplayimage as storedisplayimage
			,it.storedisplayname as storedisplayname
			,it.storedisplaythumbnail as storedisplaythumbnail
			,it.subsidiary as subsidiary
			,it.subtype as subtype
			,it.supplylotsizingmethod as supplylotsizingmethod
			,it.supplyreplenishmentmethod as supplyreplenishmentmethod
			,it.supplytimefence as supplytimefence
			,it.supplytype as supplytype
			,it.totalquantityonhand as totalquantityonhand
			,it.totalvalue as totalvalue
			,it.transferprice as transferprice
			,it.unbuildvarianceaccount as unbuildvarianceaccount
			,it.upccode as upccode
			,it.urlcomponent as urlcomponent
			,it.usebins as usebins
			,it.usemarginalrates as usemarginalrates
			,it.vendorname as vendorname
			,it.vendreturnvarianceaccount as vendreturnvarianceaccount
			,it.[weight] as [weight]
			,it.weightunit as weightunit
			,it.wipacct as wipacct
			,it.wipvarianceacct as wipvarianceacct
			,it.yahooproductfeed as yahooproductfeed
			,getDate() as md_record_written_timestamp
			,@pipelineid	   as md_record_written_pipeline_id
			,@jobid 	   as md_transformation_job_id	
			,'DWH_NETSUITE' as md_source_system
			from std.product_x prod
			full join #netsuite_item_temp it on prod.description1=it.itemid
			left join stage.netsuite_itemsubcategory isc on it.custitem_ec_item_mk_sub_category=isc.id
			left join [stage].[netsuite_producttypecategory] ptc on it.custitem_ec_product_type_category=ptc.id
			left join [stage].[netsuite_itemcategory] ic on it.custitem_ec_item_mk_category=ic.id;

			IF OBJECT_ID('tempdb..#netsuite_product_combined_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_product_combined_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_product_combined_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select	[product_id],
				[dcs_code],
				[dcs_name],
				[description1],
				[description2],
				[merge_code],
				[merge_code_desc],
				[active],
				[category],
				[sub_category],
				[kit_category],
				[size],
				[discontinuation_date],
				[pack_size],
				[EAN],
				[sub_specific],
				[write_off],
				[ss_only],
				[keyI1],
				[keyI2],
				[keyI3],
				[DG],
				[pricelist_category],
				[pricelist_sub_category],
				[base_sku],
				[product_type_cat],
				[product_type_sub_cat],
				[status],
				[item_class],
				[item_sub_class],
				[last_updated_date],
				[netsuite_id],
				[is_lot_numbered],
				[tmall_tax_code],
				[barcode_type],
				[item_form],
				[short_description],
				[display_name],
				[dg_class],
				[dg_packing_group],
				[dg_un_number],
				[container],
				[full_carton_weight],
				[inner_carton],
				[item_gross_weight],
				[item_net_weight],
				[item_unit],
				[outer_carton],
				[packed_unit_volume],
				[packed_unit_weight],
				[product_depth],
				[product_height],
				[product_width],
				[unit],
				[export_pallet],
				[fda_code],
				[hs_code],
				[country_of_origin],
				[standard_pallet],
				[voc_percentage],
				[synced_from_ns],
				[commodity_code],
				[shipper_width],
				[shipper_height],
				[shipper_depth],
				[tax_schedule],
				[send_to_wms],
				[netsuite_updated_date],
				[alternatedemandsourceitem],
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
				[cseg_sales_src],
				[custitem1],
				[4601_defaultwitaxcode],
				[clu_il1],
				[clu_il2],
				[clu_il3],
				[clu_il4],
				[code_of_supply],
				[ns_commodity_code],
				[cseg_aesop_eu],
				[additional_information],
				[ns_barcode_type],
				[boiling_point],
				[bom_level],
				[calculated_cost],
				[cegid_closed],
				[cegid_no_replenishment],
				[cegid_non_sellable],
				[cegid_plc_available],
				[cegid_plc_hash],
				[cegid_plc_not_available],
				[dg_hazchem_code],
				[dg_packing_instruction],
				[dg_proper_shipping_name],
				[dg_technical_name_nos],
				[english_registered],
				[exclude_from_3pl_rec],
				[flash_point],
				[fm_special_pricing],
				[formu_currentformulation],
				[hybris_benefit],
				[hybris_form],
				[hybris_last_modified],
				[hybris_product_category],
				[hybris_product_sub_cat],
				[hybris_range],
				[hybris_skin_type],
				[insurancestock],
				[ns_item_class],
				[item_cofa_test_odour],
				[item_dp_abc],
				[item_dp_label_direction],
				[item_dp_labelsize],
				[item_dp_leadtimevariancep],
				[item_eoblend_cofa_test_app],
				[item_exp_pallet],
				[item_fda_code],
				[item_hybris_cpsf],
				[item_hybris_deaflag],
				[item_hybris_sellableonline],
				[item_hybris_size],
				[item_manufacture_labelcode],
				[item_master_loc],
				[item_merge_code],
				[item_merge_code_backup],
				[item_mf_prodbatchsize1],
				[item_mf_prodbatchsize2],
				[item_mf_prodbatchsize3],
				[item_mk_category],
				[item_mk_item_class],
				[item_mk_item_sub_class],
				[item_mk_sub_category],
				[item_pk_compliance_check],
				[item_pk_energy_use],
				[item_pk_origin],
				[item_pk_recycled_content],
				[item_pk_water_use],
				[item_product_status],
				[item_productvarianttype],
				[item_ps_container],
				[item_ps_dangerousgood],
				[item_ps_fullboxweight],
				[item_ps_grossweight],
				[item_ps_hscode],
				[item_ps_hscode2],
				[item_ps_inner],
				[item_ps_netweight],
				[item_ps_origin],
				[item_ps_outer],
				[item_ps_packedunitvolume],
				[item_ps_packedunitweight],
				[item_ps_pallet],
				[item_ps_productheight],
				[item_ps_productlength],
				[item_ps_productwidthdiamet],
				[item_ps_retailpacksize],
				[item_ps_unit],
				[item_rm_inciname],
				[item_rm_rminternalname],
				[item_shipper_item],
				[item_size],
				[item_stock_description],
				[item_sync_to_3pl],
				[item_test_cofa_colour],
				[item_un_number],
				[ns_item_unit],
				[item_use2nd_coef],
				[item_ws_approval_date],
				[item_ws_approval_number],
				[mf_bom_item],
				[mf_work_order_item],
				[natura_code],
				[not_forecasted],
				[packaging_family],
				[packing_group],
				[packing_shipperconfig],
				[packing_spcode],
				[parent_code],
				[plc_cegid_code],
				[pricing_group],
				[product_expiry],
				[product_sales_category],
				[product_type_category],
				[product_type_sub_category],
				[record_status],
				[refill_qty],
				[rp_alternate_item],
				[sample_pack_size],
				[send_to_hybris],
				[ns_short_description],
				[supplied],
				[sync_to_fm_dp],
				[sync_to_fm_drp],
				[ns_tmall_tax_code],
				[il_ei_is_abbuono],
				[il_ei_is_premio],
				[il_ei_is_stamp],
				[il_ei_spesa_accessoria],
				[itr_supplementary_unit],
				[itr_supplementary_unit_abbrev],
				[nature_of_transaction_codes],
				[prompt_payment_discount_item],
				[reg_required],
				[sps_item_synch],
				[type_of_goods],
				[un_number],
				[voc_perc],
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
				[storedetaileddescription],
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
				[yahooproductfeed] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (
				SELECT distinct *, rank() OVER (PARTITION BY itemid ORDER BY [lastmodifieddate] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_product_combined )a WHERE dupcnt=1 ;

			truncate table std.netsuite_product_combined;
			
			insert into std.netsuite_product_combined
			select * from #netsuite_product_combined_temp
			OPTION (LABEL = 'AADSTDPRDCOM');

			DROP TABLE #netsuite_product_combined_temp;
			DROP TABLE #netsuite_item_temp;
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDPRDCOM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[netsuite_product_combined] ;
			
			delete from std.netsuite_product_combined where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_netsuite_product_combined' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END