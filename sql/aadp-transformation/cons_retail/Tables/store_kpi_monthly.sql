/****** Object:  Table [cons_retail].[store_kpi_monthly]    Script Date: 12/5/2022 8:32:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[store_kpi_monthly]
(
	[storekpikey] [varchar](250) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
	[location_key] [varchar](10) NULL,
	[date_key] [date] NULL,
	[revenue_in_aud] [float] NULL,
	[revenue_in_aud_LY] [float] NULL,
	[target_aud] [float] NULL,
	[target_aud_LY] [float] NULL,
	[budget_aud] [float] NULL,
	[budget_aud_LY] [float] NULL,
	[transactions] [int] NULL,
	[transactions_LY] [int] NULL,
	[multi_unit_transactions] [int] NULL,
	[multi_unit_transactions_LY] [int] NULL,
	[new_customer_transactions] [int] NULL,
	[new_customer_transactions_LY] [int] NULL,
	[linked_transactions] [int] NULL,
	[linked_transactions_LY] [int] NULL,
	[multi_category_transactions] [int] NULL,
	[multi_category_transactions_LY] [int] NULL,
	[units] [int] NULL,
	[units_LY] [int] NULL,
	[traffic] [int] NULL,
	[traffic_LY] [int] NULL,
	[shopfront_traffic_open] [int] NULL,
	[shopfront_traffic_open_LY] [int] NULL,
	[shopfront_traffic_closed] [int] NULL,
	[shopfront_traffic_closed_LY] [int] NULL,
	[bounces60] [int] NULL,
	[bounces60_LY] [int] NULL,
	[bounces120] [int] NULL,
	[bounces120_LY] [int] NULL,
	[in_store_secs] [int] NULL,
	[in_store_secs_LY] [int] NULL,
	[skincare_revenue_aud] [float] NULL,
	[skincare_revenue_aud_LY] [float] NULL,
	[bodycare_revenue_aud] [float] NULL,
	[bodycare_revenue_aud_LY] [float] NULL,
	[fragrance_revenue_aud] [float] NULL,
	[fragrance_revenue_aud_LY] [float] NULL,
	[haircare_revenue_aud] [float] NULL,
	[haircare_revenue_aud_LY] [float] NULL,
	[home_revenue_aud] [float] NULL,
	[home_revenue_aud_LY] [float] NULL,
	[kits_revenue_aud] [float] NULL,
	[kits_revenue_aud_LY] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [storekpikey] ),
	CLUSTERED INDEX ( [storekpikey] )
)
GO