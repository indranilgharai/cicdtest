/****** Object:  StoredProcedure [std].[sp_subsidiary]    Script Date: 4/20/2022 7:29:55 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_subsidiary] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			INSERT INTO [std].[netsuite_subsidiary]
			select distinct 
			country as country
			,currency as currency
			,custrecord_anx_b2b_default_class as anx_b2b_default_class
			,custrecord_anx_b2b_default_dep as anx_b2b_default_dep
			,custrecord_anx_b2b_default_location as anx_b2b_default_location
			,custrecord_anx_enable_advanced_email as anx_enable_advanced_email
			,custrecord_clu_il21  as il21
			,custrecord_clu_il22  as il22
			,custrecord_clu_il23  as il23
			,custrecord_clu_il24  as il24
			,custrecord_clu_il25  as il25
			,custrecord_clu_il26  as il26
			,custrecord_clu_il27  as il27
			,custrecord_clu_il28  as il28
			,custrecord_clu_il29  as il29
			,custrecord_clu_il30  as il30
			,custrecord_clu_il31  as il31
			,custrecord_clu_il32  as il32
			,custrecord_clu_il33  as il33
			,custrecord_clu_il34  as il34
			,custrecord_clu_il35  as il35
			,custrecord_clu_il36  as il36
			,custrecord_clu_il38  as il38
			,custrecord_clu_il39  as il39
			,custrecord_clu_il40  as il40
			,custrecord_clu_il42  as il42
			,custrecord_clu_il44  as il44
			,custrecord_clu_il_address as [address]
			,custrecord_clu_il_administration_ref as administration_ref
			,custrecord_clu_il_city as city
			,custrecord_clu_il_fiscal_representative as fiscal_representative
			,custrecord_clu_il_ic_rch_location as ic_rch_location
			,custrecord_clu_il_is_non_one_world as is_non_one_world
			,custrecord_clu_il_pec_email_company as pemacompany
			,custrecord_clu_il_stable_organization as stable_organization
			,custrecord_clu_il_state as state
			,custrecord_clu_il_subsidiary as subsidiary
			,custrecord_clu_il_zip as zip
			,custrecord_cn_trans_code as cn_trans_code
			,custrecord_company_brn as company_brn
			,custrecord_company_uen as company_uen
			,custrecord_digital_stocktake_complete as digital_stocktake_complete
			,custrecord_ec_additional_vat as additional_vat
			,custrecord_ec_b2b_default_location as b2b_default_location
			,custrecord_ec_b2b_default_sotype as b2b_default_sotype
			,custrecord_ec_cegid_ignore_item as cegid_ignore_item
			,custrecord_ec_cegid_requires_tax_code as cegid_requires_tax_code
			,custrecord_ec_cm_restriction as cm_restriction
			,custrecord_ec_corp_gift_disclaimer as corp_gift_disclaimer
			,custrecord_ec_dynamic_remitance_details as dynamic_remitance_details
			,custrecord_ec_enable_ra_flow as enable_ra_flow
			,custrecord_ec_fsapprover as fsapprover
			,custrecord_ec_master_reval_location as master_reval_location
			,custrecord_ec_pos_cash_sale as pos_cash_sale
			,custrecord_ec_pos_customer as pos_customer
			,custrecord_ec_remittance_details as remittance_details
			,custrecord_ec_rp_adjustment_account as rp_adjustment_account
			,custrecord_ec_rp_asn_subsidiary as rp_asn_subsidiary
			,custrecord_ec_rp_bank_dep_delay_days as rp_bank_dep_delay_days
			,custrecord_ec_rp_bank_deposit_account as rp_bank_deposit_account
			,custrecord_ec_rp_cheque_tax_code as rp_cheque_tax_code
			,custrecord_ec_rp_invoice_validation as rp_invoice_validation
			,custrecord_ec_rp_online_ship_method as rp_online_ship_method
			,custrecord_ec_rp_po_type as rp_po_type
			,custrecord_ec_rp_refund_account as rp_refund_account
			,custrecord_ec_rp_stocktake_date as rp_stocktake_date
			,custrecord_ec_rp_stocktake_time as rp_stocktake_time
			,custrecord_ec_rp_supplier as rp_supplier
			,custrecord_ec_rp_to_slip_status as rp_to_slip_status
			,custrecord_ec_rp_transfer_from_location as rp_transfer_from_location
			,custrecord_ec_rp_transfer_order_status as rp_transfer_order_status
			,custrecord_ec_rp_use_total_tax as rp_use_total_tax
			,custrecord_ec_rpro_approver as rpro_approver
			,custrecord_ec_scapprover as scapprover
			,custrecord_ec_sub_export_remittance_deta as sub_export_remittance_deta
			,custrecord_ec_sub_market_reg_enabled as sub_market_reg_enabled
			,custrecord_ec_sub_veritas_available as sub_veritas_available
			,custrecord_ec_subs_allocation_location as subs_allocation_location
			,custrecord_ec_subs_dead_stock_location as subs_dead_stock_location
			,custrecord_ec_subs_donation_location as subs_donation_location
			,custrecord_ec_subs_quarantine_location as subs_quarantine_location
			,custrecord_ec_subs_reserved_location as subs_reserved_location
			,custrecord_ec_subsidiary_cegid_enabled as subsidiary_cegid_enabled
			,custrecord_ec_subsidiary_phone as subsidiary_phone
			,custrecord_ec_warehouse_approver as warehouse_approver
			,custrecord_hc_location_flag as hc_location_flag
			,custrecord_hc_trantype_flag as hc_trantype_flag
			,custrecord_hpf_senior_mapping as hpf_senior_mapping
			,custrecord_htpfp_officer as htpfp_officer
			,custrecord_htpfp_rechecker as htpfp_rechecker
			,custrecord_htpfp_whether_mapping as htpfp_whether_mapping
			,custrecord_hybris_ns_anon_customer_id as hybris_ns_anon_customer_id
			,custrecord_il_gl_accounting_book as gl_accounting_book
			,custrecord_il_is_einvoice_2021 as is_einvoice_2021
			,custrecord_il_notification_user as notification_user
			,custrecord_jp_inv_sum_tpl_path as jp_inv_sum_tpl_path
			,custrecord_jp_isgen_individcust as jp_isgen_individcust
			,custrecord_jp_ispdf_format as jp_ispdf_format
			,custrecord_jp_loc_invsum_folder as jp_loc_invsum_folder
			,custrecord_jp_loc_sub_printed_po as jp_loc_sub_printed_po
			,custrecord_jp_loc_tax_reg_number as jp_loc_tax_reg_number
			,custrecord_jp_pdf_bank_acct_info as jp_pdf_bank_acct_info
			,custrecord_jp_pdf_greet_invoice as jp_pdf_greet_invoice
			,custrecord_jp_pdf_greet_po as jp_pdf_greet_po
			,custrecord_jp_pdf_greet_so as jp_pdf_greet_so
			,custrecord_jp_pdf_title_invoice as jp_pdf_title_invoice
			,custrecord_jp_pdf_title_journal as jp_pdf_title_journal
			,custrecord_jp_pdf_title_po as jp_pdf_title_po
			,custrecord_jp_pdf_title_so as jp_pdf_title_so
			,custrecord_jp_print_option as jp_print_option
			,custrecord_jp_printed_po_path as jp_printed_po_path
			,custrecord_online_ns_anon_customer_id as online_ns_anon_customer_id
			,custrecord_psg_lc_test_mode as psg_lc_test_mode
			,custrecord_pt_sub_taxonomy_reference as pt_sub_taxonomy_reference
			,custrecord_redbookonline_ns_anon_cust as redbookonline_ns_anon_cust
			,custrecord_rp_ns_cash_customer_id as rp_ns_cash_customer_id
			,custrecord_rp_restrict_date as rp_restrict_date
			,custrecord_rp_restrict_date_to as rp_restrict_date_to
			,custrecord_rp_sbs_no as rp_sbs_no
			,custrecord_rp_sub_enabled as rp_sub_enabled
			,custrecord_subsidiary_branch_id as subsidiary_branch_id
			,custrecord_subsidiary_chinese_name as subsidiary_chinese_name
			,custrecord_suitel10n_jp_sub_stat_search as suitel10n_jp_sub_stat_search
			,custrecord_suitel10n_jp_sub_use_holiday as suitel10n_jp_sub_use_holiday
			,custrecord_transfer_pricing_coefficient as transfer_pricing_coefficient
			,custrecord_transfer_pricing_coefficient2 as transfer_pricing_coefficient2
			,custrecord_vendor_pricing_dec_standard as vendor_pricing_dstandard
			,custrecordhybris_site as custrecordhybris_site
			,dropdownstate as dropdownstate
			,edition as edition
			,email as email
			,externalid as externalid
			,fax as fax
			,federalidnumber as federalidnumber
			,fiscalcalendar as fiscalcalendar
			,fullname as fullname
			,glimpactlocking as glimpactlocking
			,id as id
			,iselimination as iselimination
			,isinactive as isinactive
			,languagelocale as languagelocale
			,lastmodifieddate as languagelocale
			,legalname as legalname
			,mainaddress as mainaddress
			,[name] as [name]
			,parent as parent
			,purchaseorderamount as purchaseorderamount
			,purchaseorderquantity as purchaseorderquantity
			,purchaseorderquantitydiff as purchaseorderquantitydiff
			,receiptamount as receiptamount
			,receiptquantity as receiptquantity
			,receiptquantitydiff as receiptquantitydiff
			,representingcustomer as representingcustomer
			,representingvendor as representingvendor
			,returnaddress as returnaddress
			,shippingaddress as shippingaddress
			,showsubsidiaryname as showsubsidiaryname
			,ssnortin as ssnortin
			,[state] as [state] 
			,state1taxnumber as state1taxnumber
			,traninternalprefix as traninternalprefix
			,tranprefix as tranprefix
			,[url] as [url]
			,getdate() as md_record_written_timestamp
			,@pipelineid AS md_record_written_pipeline_id
			,@jobid AS md_transformation_job_id
			,'NETSUITE' as md_source_system 
			from [stage].[netsuite_subsidiary];

			IF OBJECT_ID('tempdb..#netsuite_subsidiary_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_subsidiary_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_subsidiary_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select  [country] ,
				[currency],
				[anx_b2b_default_class],
				[anx_b2b_default_dep],
				[anx_b2b_default_location],
				[anx_enable_advanced_email] ,
				[il21] ,
				[il22] ,
				[il23] ,
				[il24] ,
				[il25] ,
				[il26] ,
				[il27],
				[il28],
				[il29],
				[il30],
				[il31] ,
				[il32],
				[il33] ,
				[il34] ,
				[il35],
				[il36],
				[il38] ,
				[il39] ,
				[il40] ,
				[il42] ,
				[il44] ,
				[address] ,
				[administration_ref] ,
				[city] ,
				[fiscal_representative] ,
				[ic_rch_location],
				[is_non_one_world] ,
				[pemacompany] ,
				[stable_organization] ,
				[il_state] ,
				[subsidiary] ,
				[zip] ,
				[cn_trans_code] ,
				[company_brn] ,
				[company_uen] ,
				[digital_stocktake_complete] ,
				[additional_vat] ,
				[b2b_default_location],
				[b2b_default_sotype],
				[cegid_ignore_item] ,
				[cegid_requires_tax_code] ,
				[cm_restriction] ,
				[corp_gift_disclaimer],
				[dynamic_remitance_details] ,
				[enable_ra_flow] ,
				[fsapprover],
				[master_reval_location],
				[pos_cash_sale] ,
				[pos_customer],
				[remittance_details] ,
				[rp_adjustment_account],
				[rp_asn_subsidiary],
				[rp_bank_dep_delay_days],
				[rp_bank_deposit_account],
				[rp_cheque_tax_code],
				[rp_invoice_validation] ,
				[rp_online_ship_method],
				[rp_po_type],
				[rp_refund_account],
				[rp_stocktake_date],
				[rp_stocktake_time],
				[rp_supplier],
				[rp_to_slip_status],
				[rp_transfer_from_location],
				[rp_transfer_order_status],
				[rp_use_total_tax] ,
				[rpro_approver],
				[scapprover],
				[sub_export_remittance_deta] ,
				[sub_market_reg_enabled] ,
				[sub_veritas_available] ,
				[subs_allocation_location],
				[subs_dead_stock_location],
				[subs_donation_location],
				[subs_quarantine_location],
				[subs_reserved_location],
				[subsidiary_cegid_enabled] ,
				[subsidiary_phone] ,
				[warehouse_approver],
				[hc_location_flag] ,
				[hc_trantype_flag] ,
				[hpf_senior_mapping] ,
				[htpfp_officer] ,
				[htpfp_rechecker] ,
				[htpfp_whether_mapping] ,
				[hybris_ns_anon_customer_id],
				[gl_accounting_book],
				[is_einvoice_2021] ,
				[notification_user],
				[jp_inv_sum_tpl_path] ,
				[jp_isgen_individcust] ,
				[jp_ispdf_format],
				[jp_loc_invsum_folder] ,
				[jp_loc_sub_printed_po] ,
				[jp_loc_tax_reg_number] ,
				[jp_pdf_bank_acct_info] ,
				[jp_pdf_greet_invoice],
				[jp_pdf_greet_po],
				[jp_pdf_greet_so],
				[jp_pdf_title_invoice],
				[jp_pdf_title_journal],
				[jp_pdf_title_po],
				[jp_pdf_title_so],
				[jp_print_option],
				[jp_printed_po_path] ,
				[online_ns_anon_customer_id],
				[psg_lc_test_mode] ,
				[pt_sub_taxonomy_reference],
				[redbookonline_ns_anon_cust],
				[rp_ns_cash_customer_id],
				[rp_restrict_date],
				[rp_restrict_date_to],
				[rp_sbs_no],
				[rp_sub_enabled] ,
				[subsidiary_branch_id] ,
				[subsidiary_chinese_name] ,
				[suitel10n_jp_sub_stat_search],
				[suitel10n_jp_sub_use_holiday] ,
				[transfer_pricing_coefficient],
				[transfer_pricing_coefficient2],
				[vendor_pricing_dstandard],
				[custrecordhybris_site] ,
				[dropdownstate] ,
				[edition] ,
				[email] ,
				[externalid] ,
				[fax] ,
				[federalidnumber] ,
				[fiscalcalendar],
				[fullname] ,
				[glimpactlocking] ,
				[id],
				[iselimination] ,
				[isinactive] ,
				[languagelocale] ,
				[lastmodifieddate],
				[legalname] ,
				[mainaddress],
				[name] ,
				[parent],
				[purchaseorderamount],
				[purchaseorderquantity],
				[purchaseorderquantitydiff],
				[receiptamount],
				[receiptquantity],
				[receiptquantitydiff],
				[representingcustomer],
				[representingvendor],
				[returnaddress],
				[shippingaddress],
				[showsubsidiaryname] ,
				[ssnortin] ,
				[state] ,
				[state1taxnumber] ,
				[traninternalprefix] ,
				[tranprefix] ,
				[url] ,
				[md_record_written_timestamp],
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system]		
			from (SELECT *, rank() OVER (PARTITION BY id ORDER BY [lastmodifieddate] desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_subsidiary )a WHERE dupcnt=1 ;

			truncate table std.netsuite_subsidiary;
			
			insert into std.netsuite_subsidiary
			select * from #netsuite_subsidiary_temp			
			OPTION (LABEL = 'AADSTDSBS');

			DROP TABLE #netsuite_subsidiary_temp;
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDSBS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_subsidiary;
			
			delete from std.netsuite_subsidiary where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_netsuite_subsidiary' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END