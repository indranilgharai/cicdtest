SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_store_sales]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[invc_sid] [nvarchar](max) NULL,
	[item_pos] [nvarchar](max) NULL,
	[invc_no] [nvarchar](max) NULL,
	[invc_type] [nvarchar](max) NULL,
	[cashier_id] [nvarchar](max) NULL,
	[associate_id] [nvarchar](max) NULL,
	[created_date] [datetime2](7) NULL,
	[created_time] [datetime2](7) NULL,
	[yyyymmdd] [nvarchar](max) NULL,
	[month_no] [nvarchar](max) NULL,
	[year] [int] NULL,
	[fy] [int] NULL,
	[qtr] [nvarchar](max) NULL,
	[week_no] [nvarchar](max) NULL,
	[day_no] [nvarchar](max) NULL,
	[hour] [nvarchar](max) NULL,
	[item_sid] [nvarchar](max) NULL,
	[description1] [nvarchar](max) NULL,
	[description2] [nvarchar](max) NULL,
	[merge_code] [nvarchar](max) NULL,
	[category] [nvarchar](max) NULL,
	[sub_category] [nvarchar](max) NULL,
	[product_type_category] [nvarchar](max) NULL,
	[product_type_sub_category] [nvarchar](max) NULL,
	[qty] [bigint] NULL,
	[price] [float] NULL,
	[tax_amt] [float] NULL,
	[tax_amt2] [float] NULL,
	[line_total] [float] NULL,
	[cust_sid] [nvarchar](max) NULL,
	[posflag] [nvarchar](max) NULL,
	[price_lvl] [int] NULL,
	[fps_id] [nvarchar](max) NULL,
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