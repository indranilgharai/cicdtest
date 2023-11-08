SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_store_budget_monthly]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[budget_year] [int] NULL,
	[budget_fy] [int] NULL,
	[budget_month] [nvarchar](max) NULL,
	[sales_budget] [float] NULL,
	[mon_contribution] [float] NULL,
	[tue_contribution] [float] NULL,
	[wed_contribution] [float] NULL,
	[thu_contribution] [float] NULL,
	[fri_contribution] [float] NULL,
	[sat_contribution] [float] NULL,
	[sun_contribution] [float] NULL,
	[rent_amount] [float] NULL,
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


