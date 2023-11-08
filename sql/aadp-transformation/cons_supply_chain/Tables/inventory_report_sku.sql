-- -- Updated the indexing from heap to CLUSTERED COLUMNSTORE INDEX and distribution to Hash from round robin
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_supply_chain].[inventory_report_sku]
(
	[observation_date] [date] NULL,
	[sku] [varchar](100) NULL,
	[sku_description] [varchar](200) NULL,
	[product_category] [varchar](100) NULL,
	[product_type] [varchar](100) NULL,
	[product_life_cycle] [varchar](100) NULL,
	[inventory_location] [varchar](10) NULL,
	[inventory_location_name] [varchar](200) NULL,
	[inventory_location_category] [varchar](100) NULL,
	[region] [varchar](100) NULL,
	[subsidiary] [varchar](100) NULL,
	[stock_status] [varchar](100) NULL,
	[stock_on_hand] [float] NULL,
	[stock_in_transit] [float] NULL,
	[available_to_promise_atp] [float] NULL,
	[commit_stock] [float] NULL,
	[stock_on_hand_value] [float] NULL,
	[stock_in_transit_value] [float] NULL,
	[stock_level] [varchar](100) NULL,
	[store_coverage] [float] NULL,
	[warehouse_coverage] [float] NULL,
	[safety_stock] [float] NULL,
	[min_level] [float] NULL,
	[max_level] [float] NULL,
	[sales_units_last_28days] [float] NULL,
	[daily_sales_rate_last_28days] [float] NULL,
	[merge_code] [varchar](100) NULL,
	[source_name] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [sku] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO