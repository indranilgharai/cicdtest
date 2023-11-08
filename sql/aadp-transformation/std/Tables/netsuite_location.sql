SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_location]
(
	[custrecord1] [bigint] NULL,
	[custrecord_5826_loc_branch_id] [nvarchar](500) NULL,
	[custrecord_ec_3pl_warehouse_id] [bigint] NULL,
	[custrecord_ec_auto_asn] [nvarchar](500) NULL,
	[custrecord_ec_auto_complete_transfer_ord] [nvarchar](500) NULL,
	[custrecord_ec_average_rcv_days] [bigint] NULL,
	[custrecord_ec_cd_location] [bigint] NULL,
	[custrecord_ec_cegid_asn_to_receipt] [nvarchar](500) NULL,
	[custrecord_ec_cegid_auto_counter_payment] [nvarchar](500) NULL,
	[custrecord_ec_cegid_auto_receipt] [nvarchar](500) NULL,
	[custrecord_ec_cegid_counter_posm_cust] [bigint] NULL,
	[custrecord_ec_cegid_counter_posm_orders] [nvarchar](500) NULL,
	[custrecord_ec_cegid_ignore] [nvarchar](500) NULL,
	[custrecord_ec_cegid_ignore_snapshot] [nvarchar](500) NULL,
	[custrecord_ec_cust_class] [nvarchar](500) NULL,
	[custrecord_ec_cust_class_default] [bigint] NULL,
	[custrecord_ec_cust_department] [nvarchar](500) NULL,
	[custrecord_ec_cust_department_default] [bigint] NULL,
	[custrecord_ec_dept_store_customer] [bigint] NULL,
	[custrecord_ec_edi_invoice] [nvarchar](500) NULL,
	[custrecord_ec_fmdp_key_customer] [bigint] NULL,
	[custrecord_ec_interim_destination] [bigint] NULL,
	[custrecord_ec_invadj_class] [nvarchar](500) NULL,
	[custrecord_ec_invadj_class_default] [bigint] NULL,
	[custrecord_ec_invadj_department] [nvarchar](500) NULL,
	[custrecord_ec_invadj_department_default] [bigint] NULL,
	[custrecord_ec_is_italy_dept_store] [nvarchar](500) NULL,
	[custrecord_ec_item_warehousecode] [bigint] NULL,
	[custrecord_ec_je_class] [nvarchar](500) NULL,
	[custrecord_ec_je_class_default] [bigint] NULL,
	[custrecord_ec_je_department] [nvarchar](500) NULL,
	[custrecord_ec_je_department_default] [bigint] NULL,
	[custrecord_ec_loc_cl_date] [datetime] NULL,
	[custrecord_ec_loc_commision] [float] NULL,
	[custrecord_ec_loc_market_reg_enabled] [nvarchar](500) NULL,
	[custrecord_ec_loc_op_date] [datetime] NULL,
	[custrecord_ec_loc_region] [bigint] NULL,
	[custrecord_ec_loc_report_city] [nvarchar](500) NULL,
	[custrecord_ec_loc_veritas_available] [nvarchar](500) NULL,
	[custrecord_ec_location_cegid_enabled] [nvarchar](500) NULL,
	[custrecord_ec_location_code] [nvarchar](500) NULL,
	[custrecord_ec_location_internal_id] [bigint] NULL,
	[custrecord_ec_location_sent_to_cegid] [nvarchar](500) NULL,
	[custrecord_ec_manufacturing_dc] [bigint] NULL,
	[custrecord_ec_manufacturing_quarantine] [bigint] NULL,
	[custrecord_ec_manufacturing_reserved] [bigint] NULL,
	[custrecord_ec_manufacturing_retains] [bigint] NULL,
	[custrecord_ec_order_priority] [bigint] NULL,
	[custrecord_ec_phone] [nvarchar](500) NULL,
	[custrecord_ec_pos_hybris_controlled] [nvarchar](500) NULL,
	[custrecord_ec_pos_hybris_live_date] [datetime] NULL,
	[custrecord_ec_pos_location_latitude] [nvarchar](500) NULL,
	[custrecord_ec_pos_location_longitude] [nvarchar](500) NULL,
	[custrecord_ec_replenishment_day] [nvarchar](500) NULL,
	[custrecord_ec_replenishment_frequency] [bigint] NULL,
	[custrecord_ec_replenishment_start_date] [datetime] NULL,
	[custrecord_ec_reporting_country] [bigint] NULL,
	[custrecord_ec_reporting_region] [bigint] NULL,
	[custrecord_ec_reval_class] [bigint] NULL,
	[custrecord_ec_reval_department] [bigint] NULL,
	[custrecord_ec_rp_adj_loc_class] [bigint] NULL,
	[custrecord_ec_rp_adj_loc_department] [bigint] NULL,
	[custrecord_ec_rp_ignore] [nvarchar](500) NULL,
	[custrecord_ec_rp_ignore_pay_ref] [nvarchar](500) NULL,
	[custrecord_ec_rp_invc_loc_class] [bigint] NULL,
	[custrecord_ec_rp_invc_loc_department] [bigint] NULL,
	[custrecord_ec_rp_online_ship_0_tax_code] [bigint] NULL,
	[custrecord_ec_rp_po_loc_class] [bigint] NULL,
	[custrecord_ec_rp_po_loc_department] [bigint] NULL,
	[custrecord_ec_rp_resync_stocktake] [nvarchar](500) NULL,
	[custrecord_ec_rp_tax_code] [bigint] NULL,
	[custrecord_ec_rp_use_online_mapping] [nvarchar](500) NULL,
	[custrecord_ec_rp_use_shipping_address] [nvarchar](500) NULL,
	[custrecord_ec_sales_src_enabled] [nvarchar](500) NULL,
	[custrecord_ec_send_to_3pl] [nvarchar](500) NULL,
	[custrecord_ec_send_to_3pl_method] [bigint] NULL,
	[custrecord_ec_send_to_3pl_record_types] [nvarchar](500) NULL,
	[custrecord_ec_supplier_receiving_email] [nvarchar](500) NULL,
	[custrecord_ec_supplier_receiving_hours] [nvarchar](500) NULL,
	[custrecord_ec_sync_to_fm] [nvarchar](500) NULL,
	[custrecord_ec_to_class] [nvarchar](500) NULL,
	[custrecord_ec_to_class_default] [bigint] NULL,
	[custrecord_ec_to_department] [nvarchar](500) NULL,
	[custrecord_ec_to_department_default] [bigint] NULL,
	[custrecord_ec_vas_codes] [nvarchar](500) NULL,
	[custrecord_ec_vend_class] [nvarchar](500) NULL,
	[custrecord_ec_vend_class_default] [bigint] NULL,
	[custrecord_ec_vend_department] [nvarchar](500) NULL,
	[custrecord_ec_vend_department_default] [bigint] NULL,
	[custrecord_ec_wh_ship_detail] [bigint] NULL,
	[custrecord_hc_location_group] [bigint] NULL,
	[custrecord_rp_store_id] [bigint] NULL,
	[custrecord_sps_location_code] [nvarchar](500) NULL,
	[custrecord_sps_location_code_qual] [nvarchar](500) NULL,
	[defaultallocationpriority] [float] NULL,
	[externalid] [nvarchar](500) NULL,
	[fullname] [nvarchar](500) NULL,
	[id] [bigint] NULL,
	[includeinsupplyplanning] [nvarchar](500) NULL,
	[isinactive] [nvarchar](500) NULL,
	[lastmodifieddate] [datetime] NULL,
	[latitude] [float] NULL,
	[locationtype] [bigint] NULL,
	[longitude] [float] NULL,
	[mainaddress] [bigint] NULL,
	[makeinventoryavailable] [nvarchar](500) NULL,
	[makeinventoryavailablestore] [nvarchar](500) NULL,
	[name] [nvarchar](500) NULL,
	[parent] [bigint] NULL,
	[returnaddress] [bigint] NULL,
	[subsidiary] [nvarchar](500) NULL,
	[traninternalprefix] [nvarchar](500) NULL,
	[tranprefix] [nvarchar](500) NULL,
	[usebins] [nvarchar](500) NULL,
	[md_record_written_timestamp] [nvarchar](500) NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [nvarchar](500) NULL
)
WITH
(
DISTRIBUTION = REPLICATE,
HEAP
)
GO