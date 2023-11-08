/****** Object:  Table [cons_retail].[store_kpi_hourly]    Script Date: 12/5/2022 8:30:22 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[store_kpi_hourly]
(
	[storekpihourlykey] [varchar](250) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
	[storekpikey] [varchar](250) NOT NULL,
	[location_key] [varchar](10) NULL,
	[date_key] [datetime] NULL,
	[purchase_hour] [int] NULL,
	[transactions] [int] NULL,
	[multi_unit_transactions] [int] NULL,
	[new_customer_transactions] [int] NULL,
	[linked_transactions] [int] NULL,
	[multi_category_transactions] [int] NULL,
	[units] [int] NULL,
	[traffic] [int] NULL,
	[shopfront_traffic_open] [int] NULL,
	[shopfront_traffic_closed] [int] NULL,
	[bounces60] [int] NULL,
	[bounces120] [int] NULL,
	[in_store_secs] [bigint] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [storekpihourlykey] ),
	CLUSTERED INDEX ( [storekpihourlykey] )
)
GO