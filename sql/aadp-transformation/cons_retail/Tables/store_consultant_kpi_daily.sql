/****** Object:  Table [cons_retail].[store_consultant_kpi_daily]    Script Date: 12/5/2022 8:22:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[store_consultant_kpi_daily]
(
	[storekpiconsultantkey] [varchar](250) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
	[storekpikey] [varchar](250) NOT NULL,
	[consultant_key] [varchar](10) NULL,
	[location_key] [varchar](10) NULL,
	[date_key] [varchar](50) NULL,
	[revenue_in_aud] [float] NULL,
	[transactions] [int] NULL,
	[linked_transactions] [int] NULL,
	[multi_unit_transactions] [int] NULL,
	[multi_category_transactions] [int] NULL,
	[units] [int] NULL,
	[skincare_revenue_aud] [float] NULL,
	[bodycare_revenue_aud] [float] NULL,
	[fragrance_revenue_aud] [float] NULL,
	[haircare_revenue_aud] [float] NULL,
	[home_revenue_aud] [float] NULL,
	[kits_revenue_aud] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [storekpiconsultantkey] ),
	CLUSTERED INDEX ( [storekpiconsultantkey] )
)
GO


