SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [std].[subsidiary_x]
(
	[sbs_no] [varchar](10) NULL,
	[sbs_code] [varchar](50) NULL,
	[sbs_code_short] [varchar](50) NULL,
	[sbs_dp_code_short] [varchar](50) NULL,
	[sbs_olympic_code] [varchar](50) NULL,
	[sbs_name] [varchar](100) NULL,
	[sbs_region] [varchar](50) NULL,
	[sbs_report_region] [varchar](50) NULL,
	[sbs_currency_code] [varchar](50) NULL,
	[sbs_currency_name] [varchar](50) NULL,
	[sbs_currency_symbol] [varchar](50) NULL,
	[sbs_currency_decimal] [varchar](50) NULL,
	[sbs_currency_separator] [varchar](50) NULL,
	[gmt_offset] [int] NULL,
	[sbs_order] [int] NULL,
	[sbs_fy_start] [int] NULL,
	[sbs_active] [varchar](10) NULL,
	[store_budgets_flag] [varchar](10) NULL,
	[support_email] [varchar](100) NULL,
	[hybris_site_id] [varchar](100) NULL,
	[sbs_warehouse] [varchar](100) NULL,
	[sbs_warehouse_code] [varchar](50) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](30) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO