SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_location_combined]
(
	[sbs_no] [varchar](500) NULL,
	[store_no] [varchar](500) NULL,
	[netsuite_location] [varchar](500) NULL,
	[store_name] [varchar](500) NULL,
	[pos_terminals] [varchar](500) NULL,
	[exclude] [varchar](500) NULL,
	[city] [varchar](500) NULL,
	[hub_city] [varchar](500) NULL,
	[state] [varchar](500) NULL,
	[sbs_region] [varchar](500) NULL,
	[address1] [varchar](500) NULL,
	[address2] [varchar](500) NULL,
	[postcode] [varchar](500) NULL,
	[phone] [varchar](500) NULL,
	[trading] [varchar](500) NULL,
	[trading_veritas] [varchar](500) NULL,
	[status] [varchar](500) NULL,
	[opening_date] [varchar](500) NULL,
	[closing_date] [varchar](500) NULL,
	[open_date] [varchar](500) NULL,
	[close_date] [varchar](500) NULL,
	[open_months] [varchar](500) NULL,
	[store_or_counter] [varchar](500) NULL,
	[channel] [varchar](500) NULL,
	[store_type] [varchar](500) NULL,
	[store_format] [varchar](500) NULL,
	[location_type] [varchar](500) NULL,
	[floor_space] [varchar](500) NULL,
	[tax_rate] [varchar](500) NULL,
	[total_floor_space] [varchar](500) NULL,
	[counter_type] [varchar](500) NULL,
	[mall_id] [varchar](500) NULL,
	[lfl] [varchar](500) NULL,
	[default_language] [varchar](500) NULL,
	[iso_default_language] [varchar](500) NULL,
	[location_code] [varchar](500) NULL,
	[custrecord1] [varchar](500) NULL,
	[5826_loc_branch_id] [varchar](500) NULL,
	[3pl_warehouse_id] [int] NULL,
	[auto_asn] [varchar](500) NULL,
	[auto_complete_transfer_ord] [varchar](500) NULL,
	[average_rcv_days] [int] NULL,
	[cd_location] [int] NULL,
	[cegid_asn_to_receipt] [varchar](500) NULL,
	[cegid_auto_counter_payment] [varchar](500) NULL,
	[cegid_auto_receipt] [varchar](500) NULL,
	[cegid_counter_posm_cust] [int] NULL,
	[cegid_counter_posm_orders] [varchar](500) NULL,
	[cegid_ignore] [varchar](500) NULL,
	[cegid_ignore_snapshot] [varchar](500) NULL,
	[cust_class] [varchar](500) NULL,
	[cust_class_default] [int] NULL,
	[cust_department] [varchar](500) NULL,
	[cust_department_default] [int] NULL,
	[dept_store_customer] [int] NULL,
	[edi_invoice] [varchar](500) NULL,
	[fmdp_key_customer] [int] NULL,
	[interim_destination] [int] NULL,
	[invadj_class] [varchar](500) NULL,
	[invadj_class_default] [int] NULL,
	[invadj_department] [varchar](500) NULL,
	[invadj_department_default] [int] NULL,
	[is_italy_dept_store] [varchar](500) NULL,
	[item_warehousecode] [varchar](500) NULL,
	[je_class] [varchar](500) NULL,
	[je_class_default] [int] NULL,
	[je_department] [varchar](500) NULL,
	[je_department_default] [int] NULL,
	[loc_cl_date] [datetime2](7) NULL,
	[loc_commision] [float] NULL,
	[loc_market_reg_enabled] [varchar](500) NULL,
	[loc_op_date] [datetime2](7) NULL,
	[loc_region] [nvarchar](500) NULL,
	[loc_report_city] [varchar](500) NULL,
	[loc_veritas_available] [varchar](500) NULL,
	[location_cegid_enabled] [varchar](500) NULL,
	[location_internal_id] [int] NULL,
	[location_sent_to_cegid] [varchar](500) NULL,
	[manufacturing_dc] [int] NULL,
	[manufacturing_quarantine] [int] NULL,
	[manufacturing_reserved] [int] NULL,
	[manufacturing_retains] [int] NULL,
	[order_priority] [int] NULL,
	[ns_phone] [varchar](500) NULL,
	[pos_hybris_controlled] [varchar](500) NULL,
	[pos_hybris_live_date] [datetime2](7) NULL,
	[pos_location_latitude] [varchar](500) NULL,
	[pos_location_longitude] [varchar](500) NULL,
	[replenishment_day] [varchar](500) NULL,
	[replenishment_frequency] [int] NULL,
	[replenishment_start_date] [datetime2](7) NULL,
	[reporting_country] [int] NULL,
	[reporting_region] [int] NULL,
	[reval_class] [int] NULL,
	[reval_department] [int] NULL,
	[rp_adj_loc_class] [int] NULL,
	[rp_adj_loc_department] [int] NULL,
	[rp_ignore] [varchar](500) NULL,
	[rp_ignore_pay_ref] [varchar](500) NULL,
	[rp_invc_loc_class] [int] NULL,
	[rp_invc_loc_department] [int] NULL,
	[rp_online_ship_0_tax_code] [int] NULL,
	[rp_po_loc_class] [int] NULL,
	[rp_po_loc_department] [int] NULL,
	[rp_resync_stocktake] [varchar](500) NULL,
	[rp_tax_code] [int] NULL,
	[rp_use_online_mapping] [varchar](500) NULL,
	[rp_use_shipping_address] [varchar](500) NULL,
	[sales_src_enabled] [varchar](500) NULL,
	[send_to_3pl] [varchar](500) NULL,
	[send_to_3pl_method] [int] NULL,
	[send_to_3pl_record_types] [varchar](500) NULL,
	[supplier_receiving_email] [varchar](500) NULL,
	[supplier_receiving_hours] [varchar](500) NULL,
	[sync_to_fm] [varchar](500) NULL,
	[to_class] [varchar](500) NULL,
	[to_class_default] [int] NULL,
	[to_department] [varchar](500) NULL,
	[to_department_default] [int] NULL,
	[vas_codes] [varchar](500) NULL,
	[vend_class] [varchar](500) NULL,
	[vend_class_default] [int] NULL,
	[vend_department] [varchar](500) NULL,
	[vend_department_default] [int] NULL,
	[wh_ship_detail] [int] NULL,
	[hc_location_group] [int] NULL,
	[rp_store_id] [int] NULL,
	[sps_location_code] [varchar](500) NULL,
	[sps_location_code_qual] [varchar](500) NULL,
	[defaultallocationpriority] [float] NULL,
	[externalid] [varchar](500) NULL,
	[fullname] [varchar](500) NULL,
	[id] [int] NULL,
	[includeinsupplyplanning] [varchar](500) NULL,
	[isinactive] [varchar](500) NULL,
	[lastmodifieddate] [datetime2](7) NULL,
	[latitude] [float] NULL,
	[locationtype] [varchar](500) NULL,
	[longitude] [float] NULL,
	[mainaddress] [int] NULL,
	[makeinventoryavailable] [varchar](500) NULL,
	[makeinventoryavailablestore] [varchar](500) NULL,
	[name] [varchar](500) NULL,
	[parent] [int] NULL,
	[returnaddress] [int] NULL,
	[subsidiary] [varchar](500) NULL,
	[traninternalprefix] [varchar](500) NULL,
	[tranprefix] [varchar](500) NULL,
	[usebins] [varchar](500) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [store_no] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO


