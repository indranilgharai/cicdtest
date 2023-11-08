-- ## SP for load of Standardised table : netsuite_location ##
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC std.sp_netsuite_location @jobid int,@step_number int,@reset bit,@pipelineid varchar(500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN
			DECLARE @read_count [int]
			select @read_count=count(*) from stage.netsuite_location;
			if (@read_count)>0
			BEGIN
				truncate table [std].[netsuite_location];
				insert into std.netsuite_location
				select custrecord1,
				custrecord_5826_loc_branch_id,
				custrecord_ec_3pl_warehouse_id,
				custrecord_ec_auto_asn,
				custrecord_ec_auto_complete_transfer_ord,
				custrecord_ec_average_rcv_days,
				custrecord_ec_cd_location,
				custrecord_ec_cegid_asn_to_receipt,
				custrecord_ec_cegid_auto_counter_payment,
				custrecord_ec_cegid_auto_receipt,
				custrecord_ec_cegid_counter_posm_cust,
				custrecord_ec_cegid_counter_posm_orders,
				custrecord_ec_cegid_ignore,
				custrecord_ec_cegid_ignore_snapshot,
				custrecord_ec_cust_class,
				custrecord_ec_cust_class_default,
				custrecord_ec_cust_department,
				custrecord_ec_cust_department_default,
				custrecord_ec_dept_store_customer,
				custrecord_ec_edi_invoice,
				custrecord_ec_fmdp_key_customer,
				custrecord_ec_interim_destination,
				custrecord_ec_invadj_class,
				custrecord_ec_invadj_class_default,
				custrecord_ec_invadj_department,
				custrecord_ec_invadj_department_default,
				custrecord_ec_is_italy_dept_store,
				custrecord_ec_item_warehousecode,
				custrecord_ec_je_class,
				custrecord_ec_je_class_default,
				custrecord_ec_je_department,
				custrecord_ec_je_department_default,
				custrecord_ec_loc_cl_date,
				custrecord_ec_loc_commision,
				custrecord_ec_loc_market_reg_enabled,
				custrecord_ec_loc_op_date,
				custrecord_ec_loc_region,
				custrecord_ec_loc_report_city,
				custrecord_ec_loc_veritas_available,
				custrecord_ec_location_cegid_enabled,
				custrecord_ec_location_code,
				custrecord_ec_location_internal_id,
				custrecord_ec_location_sent_to_cegid,
				custrecord_ec_manufacturing_dc,
				custrecord_ec_manufacturing_quarantine,
				custrecord_ec_manufacturing_reserved,
				custrecord_ec_manufacturing_retains,
				custrecord_ec_order_priority,
				custrecord_ec_phone,
				custrecord_ec_pos_hybris_controlled,
				custrecord_ec_pos_hybris_live_date,
				custrecord_ec_pos_location_latitude,
				custrecord_ec_pos_location_longitude,
				custrecord_ec_replenishment_day,
				custrecord_ec_replenishment_frequency,
				custrecord_ec_replenishment_start_date,
				custrecord_ec_reporting_country,
				custrecord_ec_reporting_region,
				custrecord_ec_reval_class,
				custrecord_ec_reval_department,
				custrecord_ec_rp_adj_loc_class,
				custrecord_ec_rp_adj_loc_department,
				custrecord_ec_rp_ignore,
				custrecord_ec_rp_ignore_pay_ref,
				custrecord_ec_rp_invc_loc_class,
				custrecord_ec_rp_invc_loc_department,
				custrecord_ec_rp_online_ship_0_tax_code,
				custrecord_ec_rp_po_loc_class,
				custrecord_ec_rp_po_loc_department,
				custrecord_ec_rp_resync_stocktake,
				custrecord_ec_rp_tax_code,
				custrecord_ec_rp_use_online_mapping,
				custrecord_ec_rp_use_shipping_address,
				custrecord_ec_sales_src_enabled,
				custrecord_ec_send_to_3pl,
				custrecord_ec_send_to_3pl_method,
				custrecord_ec_send_to_3pl_record_types,
				custrecord_ec_supplier_receiving_email,
				custrecord_ec_supplier_receiving_hours,
				custrecord_ec_sync_to_fm,
				custrecord_ec_to_class,
				custrecord_ec_to_class_default,
				custrecord_ec_to_department,
				custrecord_ec_to_department_default,
				custrecord_ec_vas_codes,
				custrecord_ec_vend_class,
				custrecord_ec_vend_class_default,
				custrecord_ec_vend_department,
				custrecord_ec_vend_department_default,
				custrecord_ec_wh_ship_detail,
				custrecord_hc_location_group,
				custrecord_rp_store_id,
				custrecord_sps_location_code,
				custrecord_sps_location_code_qual,
				defaultallocationpriority,
				externalid,
				fullname,
				id,
				includeinsupplyplanning,
				isinactive,
				lastmodifieddate,
				latitude,
				locationtype,
				longitude,
				mainaddress,
				makeinventoryavailable,
				makeinventoryavailablestore,
				name,
				parent,
				returnaddress,
				subsidiary,
				traninternalprefix,
				tranprefix,
				usebins,
				getDate() as md_record_written_timestamp,
				@pipelineid as md_record_written_pipeline_id,
				@jobid as md_transformation_job_id,  
				'Netsuite'
				from stage.netsuite_location
				OPTION (LABEL = 'AADPSTDLOC');

				UPDATE STATISTICS std.netsuite_location;
				UPDATE STATISTICS stage.netsuite_location;
				
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label varchar(500)
				SET @label='AADPSTDLOC'
				EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			END
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME, @onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM std.netsuite_location;
			
			DELETE FROM std.netsuite_location WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:std.netsuite_location'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_netsuite_location' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END