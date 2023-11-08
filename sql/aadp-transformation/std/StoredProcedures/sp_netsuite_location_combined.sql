/****** Modified  StoredProcedure  [Changed SP to remove name from deduplication logic] Modified by Patrick Lacerna 18/07/2023 14:20:00 ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [std].[sp_netsuite_location_combined] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			with warehousecodelist as (
			select  distinct  *,rank() OVER (PARTITION BY id order by md_record_ingestion_timestamp desc,md_record_ingestion_pipeline_id desc) as rk
			from [stage].[netsuite_warehousecodelist]
			)

			INSERT INTO [std].[netsuite_location_combined] 
			SELECT DISTINCT 
			 st.sbs_no as sbs_no
			,st.store_no AS store_no
			,st.netsuite_location AS netsuite_location
			,st.store_name AS store_name
			,st.pos_terminals AS pos_terminals
			,st.exclude AS exclude
			,st.city AS city
			,st.hub_city AS hub_city
			,st.[state]  AS [state]
			,st.sbs_region AS sbs_region
			,st.address1 AS address1
			,st.address2 AS address2
			,st.postcode AS postcode
			,st.phone AS phone--
			,st.trading AS trading
			,st.trading_veritas AS trading_veritas
			,st.[status] AS [status]
			,st.opening_date AS	 opening_date
			,st.closing_date AS	 closing_date
			,st.open_date AS open_date
			,st.close_date AS close_date
			,st.open_months AS open_months
			,st.store_or_counter AS store_or_counter
			,st.channel AS channel
			,st.store_type AS store_type
			,st.store_format AS store_format
			,st.location_type AS location_type
			,st.floor_space AS floor_space
			,st.tax_rate AS tax_rate
			,st.total_floor_space AS total_floor_space
			,st.counter_type AS counter_type
			,st.mall_id AS mall_id
			,st.lfl AS lfl
			,st.[default_language]  AS [default_language]
			,st.iso_default_language AS iso_default_language
			,COALESCE(st.location_code,loc.custrecord_ec_location_code ) AS location_code
			
			,cat.[name] AS custrecord1
			,loc.custrecord_5826_loc_branch_id AS [5826_loc_branch_id]
			,loc.custrecord_ec_3pl_warehouse_id AS [3pl_warehouse_id]
			,loc.custrecord_ec_auto_asn AS auto_asn
			,loc.custrecord_ec_auto_complete_transfer_ord AS auto_complete_transfer_ord
			,loc.custrecord_ec_average_rcv_days AS average_rcv_days
			,loc.custrecord_ec_cd_location AS cd_location
			,loc.custrecord_ec_cegid_asn_to_receipt AS cegid_asn_to_receipt
			,loc.custrecord_ec_cegid_auto_counter_payment AS cegid_auto_counter_payment
			,loc.custrecord_ec_cegid_auto_receipt AS cegid_auto_receipt
			,loc.custrecord_ec_cegid_counter_posm_cust AS cegid_counter_posm_cust
			,loc.custrecord_ec_cegid_counter_posm_orders AS cegid_counter_posm_orders
			,loc.custrecord_ec_cegid_ignore AS cegid_ignore
			,loc.custrecord_ec_cegid_ignore_snapshot AS cegid_ignore_snapshot
			,loc.custrecord_ec_cust_class AS cust_class
			,loc.custrecord_ec_cust_class_default AS cust_class_default
			,loc.custrecord_ec_cust_department AS cust_department
			,loc.custrecord_ec_cust_department_default AS cust_department_default
			,loc.custrecord_ec_dept_store_customer AS dept_store_customer
			,loc.custrecord_ec_edi_invoice AS edi_invoice
			,loc.custrecord_ec_fmdp_key_customer AS fmdp_key_customer
			,loc.custrecord_ec_interim_destination AS interim_destination
			,loc.custrecord_ec_invadj_class AS invadj_class
			,loc.custrecord_ec_invadj_class_default AS invadj_class_default
			,loc.custrecord_ec_invadj_department AS invadj_department
			,loc.custrecord_ec_invadj_department_default AS invadj_department_default
			,loc.custrecord_ec_is_italy_dept_store AS is_italy_dept_store
			,whc.[name] AS item_warehousecode
			,loc.custrecord_ec_je_class AS je_class
			,loc.custrecord_ec_je_class_default AS je_class_default
			,loc.custrecord_ec_je_department AS je_department
			,loc.custrecord_ec_je_department_default AS je_department_default
			,loc.custrecord_ec_loc_cl_date AS loc_cl_date
			,loc.custrecord_ec_loc_commision AS loc_commision
			,loc.custrecord_ec_loc_market_reg_enabled AS loc_market_reg_enabled
			,loc.custrecord_ec_loc_op_date AS loc_op_date
			,reg.[name] AS loc_region
			,loc.custrecord_ec_loc_report_city AS loc_report_city
			,loc.custrecord_ec_loc_veritas_available AS loc_veritas_available
			,loc.custrecord_ec_location_cegid_enabled AS location_cegid_enabled
			,loc.custrecord_ec_location_internal_id AS location_internal_id
			,loc.custrecord_ec_location_sent_to_cegid AS location_sent_to_cegid
			,loc.custrecord_ec_manufacturing_dc AS manufacturing_dc
			,loc.custrecord_ec_manufacturing_quarantine AS manufacturing_quarantine
			,loc.custrecord_ec_manufacturing_reserved AS manufacturing_reserved
			,loc.custrecord_ec_manufacturing_retains AS manufacturing_retains
			,loc.custrecord_ec_order_priority AS order_priority
			,loc.custrecord_ec_phone AS ns_phone
			,loc.custrecord_ec_pos_hybris_controlled AS pos_hybris_controlled
			,loc.custrecord_ec_pos_hybris_live_date AS pos_hybris_live_date
			,loc.custrecord_ec_pos_location_latitude AS pos_location_latitude
			,loc.custrecord_ec_pos_location_longitude AS pos_location_longitude
			,loc.custrecord_ec_replenishment_day AS replenishment_day
			,loc.custrecord_ec_replenishment_frequency AS replenishment_frequency
			,loc.custrecord_ec_replenishment_start_date AS replenishment_start_date
			,loc.custrecord_ec_reporting_country AS reporting_country
			,loc.custrecord_ec_reporting_region AS reporting_region
			,loc.custrecord_ec_reval_class AS reval_class
			,loc.custrecord_ec_reval_department AS reval_department
			,loc.custrecord_ec_rp_adj_loc_class AS rp_adj_loc_class
			,loc.custrecord_ec_rp_adj_loc_department AS rp_adj_loc_department
			,loc.custrecord_ec_rp_ignore AS rp_ignore
			,loc.custrecord_ec_rp_ignore_pay_ref AS rp_ignore_pay_ref
			,loc.custrecord_ec_rp_invc_loc_class AS rp_invc_loc_class
			,loc.custrecord_ec_rp_invc_loc_department AS rp_invc_loc_department
			,loc.custrecord_ec_rp_online_ship_0_tax_code AS rp_online_ship_0_tax_code
			,loc.custrecord_ec_rp_po_loc_class AS rp_po_loc_class
			,loc.custrecord_ec_rp_po_loc_department AS rp_po_loc_department
			,loc.custrecord_ec_rp_resync_stocktake AS rp_resync_stocktake
			,loc.custrecord_ec_rp_tax_code AS rp_tax_code
			,loc.custrecord_ec_rp_use_online_mapping AS rp_use_online_mapping
			,loc.custrecord_ec_rp_use_shipping_address AS rp_use_shipping_address
			,loc.custrecord_ec_sales_src_enabled AS sales_src_enabled
			,loc.custrecord_ec_send_to_3pl AS send_to_3pl
			,loc.custrecord_ec_send_to_3pl_method AS send_to_3pl_method
			,loc.custrecord_ec_send_to_3pl_record_types AS send_to_3pl_record_types
			,loc.custrecord_ec_supplier_receiving_email AS supplier_receiving_email
			,loc.custrecord_ec_supplier_receiving_hours AS supplier_receiving_hours
			,loc.custrecord_ec_sync_to_fm AS sync_to_fm
			,loc.custrecord_ec_to_class AS to_class
			,loc.custrecord_ec_to_class_default AS to_class_default
			,loc.custrecord_ec_to_department AS to_department
			,loc.custrecord_ec_to_department_default AS to_department_default
			,loc.custrecord_ec_vas_codes AS vas_codes
			,loc.custrecord_ec_vend_class AS vend_class 
			,loc.custrecord_ec_vend_class_default AS vend_class_default
			,loc.custrecord_ec_vend_department AS vend_department
			,loc.custrecord_ec_vend_department_default AS vend_department_default
			,loc.custrecord_ec_wh_ship_detail AS wh_ship_detail
			,loc.custrecord_hc_location_group AS hc_location_group
			,loc.custrecord_rp_store_id AS rp_store_id
			,loc.custrecord_sps_location_code AS sps_location_code
			,loc.custrecord_sps_location_code_qual AS sps_location_code_qual
			,loc.defaultallocationpriority AS defaultallocationpriority
			,loc.externalid AS externalid
			,loc.fullname AS fullname
			,loc.id AS id
			,loc.includeinsupplyplanning AS includeinsupplyplanning
			,loc.isinactive AS isinactive
			,loc.lastmodifieddate AS lastmodifieddate
			,loc.latitude AS latitude
			,typ.[name] AS locationtype
			,loc.longitude AS longitude
			,loc.mainaddress AS mainaddress
			,loc.makeinventoryavailable AS makeinventoryavailable
			,loc.makeinventoryavailablestore AS makeinventoryavailablestore
			,loc.[name] AS [name]
			,loc.parent AS parent
			,loc.returnaddress AS returnaddress
			,sbs.custrecord_rp_sbs_no AS subsidiary
			,loc.traninternalprefix AS traninternalprefix
			,loc.tranprefix AS tranprefix
			,loc.usebins AS usebins
			,getDate() as md_record_written_timestamp
			,@pipelineid	   as md_record_written_pipeline_id
			,@jobid 	   as md_transformation_job_id
			,'DWH_NETSUITE' as md_source_system
			from [stage].[netsuite_location] loc
			FULL JOIN std.store_x st on loc.custrecord_ec_location_code =st.location_code
			left join [stage].[netsuite_locationcategory] cat on loc.custrecord1=cat.id
			left join (select * from warehousecodelist where rk=1) whc on whc.id=loc.custrecord_ec_item_warehousecode
			left join stage.[netsuite_locationtype] typ on typ.id=loc.locationtype
			left join stage.[netsuite_regionlist] reg on reg.id=loc.custrecord_ec_loc_region
			left join stage.[netsuite_subsidiary] sbs on sbs.id=loc.subsidiary;

			IF OBJECT_ID('tempdb..#netsuite_location_combined_temp') IS NOT NULL
			BEGIN
				DROP TABLE #netsuite_location_combined_temp
			END
			
			-----------------------temporary table to deduplicate data ---------------------------
			
			create table #netsuite_location_combined_temp WITH( DISTRIBUTION = ROUND_ROBIN,HEAP) AS
			select [sbs_no],
				[store_no],
				[netsuite_location],
				[store_name],
				[pos_terminals],
				[exclude],
				[city],
				[hub_city],
				[state],
				[sbs_region],
				[address1],
				[address2],
				[postcode],
				[phone],
				[trading],
				[trading_veritas],
				[status],
				[opening_date],
				[closing_date],
				[open_date],
				[close_date],
				[open_months],
				[store_or_counter],
				[channel],
				[store_type],
				[store_format],
				[location_type],
				[floor_space],
				[tax_rate],
				[total_floor_space],
				[counter_type],
				[mall_id],
				[lfl],
				[default_language],
				[iso_default_language],
				[location_code],
				[custrecord1],
				[5826_loc_branch_id],
				[3pl_warehouse_id],
				[auto_asn],
				[auto_complete_transfer_ord],
				[average_rcv_days],
				[cd_location],
				[cegid_asn_to_receipt],
				[cegid_auto_counter_payment],
				[cegid_auto_receipt],
				[cegid_counter_posm_cust],
				[cegid_counter_posm_orders],
				[cegid_ignore],
				[cegid_ignore_snapshot],
				[cust_class],
				[cust_class_default],
				[cust_department],
				[cust_department_default],
				[dept_store_customer],
				[edi_invoice],
				[fmdp_key_customer],
				[interim_destination],
				[invadj_class],
				[invadj_class_default],
				[invadj_department],
				[invadj_department_default],
				[is_italy_dept_store],
				[item_warehousecode],
				[je_class],
				[je_class_default],
				[je_department],
				[je_department_default],
				[loc_cl_date],
				[loc_commision],
				[loc_market_reg_enabled],
				[loc_op_date],
				[loc_region],
				[loc_report_city],
				[loc_veritas_available],
				[location_cegid_enabled],
				[location_internal_id],
				[location_sent_to_cegid],
				[manufacturing_dc],
				[manufacturing_quarantine],
				[manufacturing_reserved],
				[manufacturing_retains],
				[order_priority],
				[ns_phone],
				[pos_hybris_controlled],
				[pos_hybris_live_date],
				[pos_location_latitude],
				[pos_location_longitude],
				[replenishment_day],
				[replenishment_frequency],
				[replenishment_start_date],
				[reporting_country],
				[reporting_region],
				[reval_class],
				[reval_department],
				[rp_adj_loc_class],
				[rp_adj_loc_department],
				[rp_ignore],
				[rp_ignore_pay_ref],
				[rp_invc_loc_class],
				[rp_invc_loc_department],
				[rp_online_ship_0_tax_code],
				[rp_po_loc_class],
				[rp_po_loc_department],
				[rp_resync_stocktake],
				[rp_tax_code],
				[rp_use_online_mapping],
				[rp_use_shipping_address],
				[sales_src_enabled],
				[send_to_3pl],
				[send_to_3pl_method],
				[send_to_3pl_record_types],
				[supplier_receiving_email],
				[supplier_receiving_hours],
				[sync_to_fm],
				[to_class],
				[to_class_default],
				[to_department],
				[to_department_default],
				[vas_codes],
				[vend_class],
				[vend_class_default],
				[vend_department],
				[vend_department_default],
				[wh_ship_detail],
				[hc_location_group],
				[rp_store_id],
				[sps_location_code],
				[sps_location_code_qual],
				[defaultallocationpriority],
				[externalid],
				[fullname],
				[id],
				[includeinsupplyplanning],
				[isinactive],
				[lastmodifieddate],
				[latitude],
				[locationtype],
				[longitude],
				[mainaddress],
				[makeinventoryavailable],
				[makeinventoryavailablestore],
				[name],
				[parent],
				[returnaddress],
				[subsidiary],
				[traninternalprefix],
				[tranprefix],
				[usebins] ,
				[md_record_written_timestamp] ,
				[md_record_written_pipeline_id] ,
				[md_transformation_job_id] ,
				[md_source_system] 		
			from (
				SELECT *, rank() OVER (PARTITION BY location_code ORDER BY lastmodifieddate desc,md_record_written_timestamp desc) AS dupcnt
				FROM std.netsuite_location_combined )a WHERE dupcnt=1 ;

			truncate table std.netsuite_location_combined;
			
			insert into std.netsuite_location_combined
			select * from #netsuite_location_combined_temp
			OPTION (LABEL = 'AADSTDLOCCOM');
			
			DROP TABLE #netsuite_location_combined_temp;
			UPDATE STATISTICS [std].[netsuite_location_combined]; 
			
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDLOCCOM'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].netsuite_location_combined ;
			
			delete from std.netsuite_location_combined where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_netsuite_location_combined' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END