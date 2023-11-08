SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_subsidiary_x]
(
	[sbs_no] [nvarchar](max) NULL,
	[sbs_code] [nvarchar](max) NULL,
	[sbs_code_short] [nvarchar](max) NULL,
	[sbs_dp_code_short] [nvarchar](max) NULL,
	[sbs_olympic_code] [nvarchar](max) NULL,
	[sbs_name] [nvarchar](max) NULL,
	[sbs_region] [nvarchar](max) NULL,
	[sbs_report_region] [nvarchar](max) NULL,
	[sbs_currency_code] [nvarchar](max) NULL,
	[sbs_currency_name] [nvarchar](max) NULL,
	[sbs_currency_symbol] [nvarchar](max) NULL,
	[sbs_currency_decimal] [nvarchar](max) NULL,
	[sbs_currency_separator] [nvarchar](max) NULL,
	[gmt_offset] [nvarchar](max) NULL,
	[sbs_order] [int] NULL,
	[sbs_fy_start] [nvarchar](max) NULL,
	[sbs_active] [nvarchar](max) NULL,
	[store_budgets_flag] [nvarchar](max) NULL,
	[support_email] [nvarchar](max) NULL,
	[hybris_site_id] [nvarchar](max) NULL,
	[sbs_warehouse] [nvarchar](max) NULL,
	[sbs_warehouse_code] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


