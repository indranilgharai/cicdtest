SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[dwh_store_budget_monthly]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[budget_year] [int] NULL,
	[budget_fy] [int] NULL,
	[budget_month] [int] NULL,
	[sales_budget] [float] NULL,
	[mon_contribution] [float] NULL,
	[tue_contribution] [float] NULL,
	[wed_contribution] [float] NULL,
	[thu_contribution] [float] NULL,
	[fri_contribution] [float] NULL,
	[sat_contribution] [float] NULL,
	[sun_contribution] [float] NULL,
	[rent_amount] [float] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO


