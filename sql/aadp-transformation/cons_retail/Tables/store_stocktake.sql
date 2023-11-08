/****** Object:  Table [cons_retail].[store_stocktake]    Script Date: 2/7/2023 1:04:08 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[store_stocktake]
(
	[locationkey] [varchar](20) NULL,
	[productkey] [varchar](20) NULL,
	[location_productKey] [varchar](50) NULL,
	[stocktake_name] [varchar](100) NULL,
	[stocktake_qtr] [varchar](5) NULL,
	[stocktake_year] [int] NULL,
	[stocktake_date] [varchar](20) NULL,
	[stocktake_variance_units] [bigint] NULL,
	[stocktake_variance_units_abs] [bigint] NULL,
	[item_cost_aud] [float] NULL,
	[item_rrp_aud] [float] NULL,
	[revenue_since_last_stocktake] [float] NULL,
	[units_since_last_stocktake] [bigint] NULL,
	[last_stocktake_name] [varchar](40) NULL,
	[last_stocktake_qtr] [varchar](5) NULL,
	[last_stocktake_year] [int] NULL,
	[last_stocktake_date] [varchar](20) NULL,
	[asls_aged_stock] [bigint] NULL,
	[asls_retail_customer_return] [bigint] NULL,
	[asls_damaged_in_store] [bigint] NULL,
	[asls_damaged_in_transit] [bigint] NULL,
	[asls_kit_break] [bigint] NULL,
	[asls_PQ_removal_from_sale] [bigint] NULL,
	[asls_known_theft] [bigint] NULL,
	[asls_covert_to_tester] [bigint] NULL,
	[asls_convert_for_treatment_use] [bigint] NULL,
	[asls_amenity_account_gift] [bigint] NULL,
	[asls_retail_customer_gift] [bigint] NULL,
	[asls_product_donation] [bigint] NULL,
	[asls_head_office_authorized] [bigint] NULL,
	[asls_marketing_pr_initiative] [bigint] NULL,
	[asls_staff_complimentary_product] [bigint] NULL,
	[asls_online_customer_return] [bigint] NULL,
	[asls_online_customer_gift] [bigint] NULL,
	[asls_online_lost_damaged] [bigint] NULL,
	[asls_overdelivery_from_warehouse] [bigint] NULL,
	[asls_underdelivery_from_warehouse] [bigint] NULL,
	[asls_stocktake_or_cycle_count_processing_error] [bigint] NULL,
	[asls_staff_processing_error] [bigint] NULL,
	[asls_online_staff_processing_error] [bigint] NULL,
	[asls_new_starter_allocation] [bigint] NULL,
	[asls_sachet_sample_processing_error] [bigint] NULL,
	[asls_PQ_employee] [bigint] NULL,
	[asls_PQ_customer] [bigint] NULL,
	[asls_other] [bigint] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
