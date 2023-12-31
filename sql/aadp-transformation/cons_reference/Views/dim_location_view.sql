/****** Object:  View [cons_reference].[dim_location_view]    Script Date: 1/18/2023 3:52:52 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [cons_reference].[dim_location_view] AS SELECT
    location_code as locationkey,
    cast(st.sbs_no as int) as store_sbs_no,
    cast(st.store_no as int) store_no,
    cast(st.netsuite_location as int) netsuite_location,
    cast(st.store_name as varchar(200)) store_name,
    cast(st.pos_terminals as int) pos_terminals,
    cast(st.exclude as varchar(10)) exclude,
    cast(st.city as varchar(200)) city,
    cast(st.hub_city as varchar(200)) hub_city,
    cast(st.state as varchar(100)) "state",
    st.sbs_region as store_sbs_region,
    cast(st.address1 as varchar(200)) address1,
    cast(st.address2 as varchar(200)) address2,
    cast(st.postcode as varchar(50)) postcode,
    cast(st.phone as varchar(50)) phone,
    st.trading,
    st.trading_veritas,
    st.status,
    st.opening_date,
    st.closing_date,
    cast(st.open_date as date) open_date,
    cast(st.close_date as date) close_date,
    st.open_months,
    cast(st.store_or_counter as varchar(50)) store_or_counter,
    cast(st.channel as varchar(50)) channel,
    st.store_type,
    st.store_format,
    st.location_type,
    st.floor_space,
    st.tax_rate,
    st.total_floor_space,
    st.counter_type,
    st.mall_id,
    cast(st.lfl as varchar(10)) lfl,
    cast(st.default_language as varchar(50)) "default_language",
    cast(st.iso_default_language as varchar(10)) iso_default_language,
    cast(sub.sbs_no as int) as subsidiary_sbs_no,
    sub.sbs_code,
    sub.sbs_code_short,
    sub.sbs_dp_code_short,
    sub.sbs_olympic_code,
    sub.sbs_name,
    sub.sbs_region as subsidiary_sbs_region,
    sub.sbs_report_region,
    sub.sbs_currency_code,
    sub.sbs_currency_name,
    cast(sub.sbs_currency_symbol as nvarchar(10)) sbs_currency_symbol,
    sub.sbs_currency_decimal,
    sub.sbs_currency_separator,
    sub.sbs_order,
    sub.sbs_fy_start,
    sub.sbs_active,
    sub.store_budgets_flag,
    sub.support_email,
    sub.hybris_site_id,
    sub.sbs_warehouse,
    sub.sbs_warehouse_code,
    
    cast(srd.[Retail_Business_Code] as varchar(50)) "retail_business_code",
    cast(srd.[Retail_Business_Name] as varchar(300)) "retail_business_name",
    cast(srd.[Sub_Region_Code] as varchar(50)) "sub_region_code",
    cast(srd.[Region_Code] as varchar(50)) as "region_code", 
    st.md_record_written_timestamp as store_md_record_written_timestamp,
    st.md_record_written_pipeline_id as store_md_record_written_pipeline_id,
    st.md_transformation_job_id as store_md_transformation_job_id,
    st.md_source_system as store_md_source_system,
	sub.md_record_written_timestamp as subsidiary_md_record_written_timestamp,
    sub.md_record_written_pipeline_id as subsidiary_md_record_written_pipeline_id,
    sub.md_transformation_job_id as subsidiary_md_transformation_job_id,
    sub.md_source_system as subsidiary_md_source_system,
	srd.[md_record_written_timestamp] as storeforce_md_record_written_timestamp,
    srd.[md_record_written_pipeline_id] as storeforce_md_record_written_pipeline_id,
    srd.[md_transformation_job_id] as storeforce_md_transformation_job_id
FROM
    std.store_x st
    left join std.subsidiary_x sub on sub.sbs_no = st.sbs_no
    left join std.storeforce_rbm_data srd on srd.[store_code] = st.location_code
	where location_code is not null;
GO
