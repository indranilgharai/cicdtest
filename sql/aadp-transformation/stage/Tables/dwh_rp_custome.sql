SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_rp_customer]
(
	[cust_sid] [nvarchar](max) NULL,
	[sbs_no] [int] NULL,
	[cust_id] [nvarchar](max) NULL,
	[store_no] [int] NULL,
	[home_sbs_no] [nvarchar](max) NULL,
	[home_store_no] [nvarchar](max) NULL,
	[company_id] [nvarchar](max) NULL,
	[title_id] [nvarchar](max) NULL,
	[active] [nvarchar](max) NULL,
	[mark1] [nvarchar](max) NULL,
	[mark2] [nvarchar](max) NULL,
	[udf_date1] [nvarchar](max) NULL,
	[created_date] [datetime2](7) NULL,
	[modified_date] [datetime2](7) NULL,
	[allow_email] [int] NULL,
	[fst_sale_date] [nvarchar](max) NULL,
	[lst_sale_date] [nvarchar](max) NULL,
	[lst_sale_amt] [real] NULL,
	[price_lvl] [int] NULL,
	[allow_phone] [int] NULL,
	[allow_post] [int] NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL,
	[first_name] [nvarchar](max) NULL,
	[last_name] [nvarchar](max) NULL,
	[email_addr] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO