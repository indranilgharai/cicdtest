SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_supply_chain].[inventory_report_temp]
(
	[sku] [varchar](100) NULL,
	[sku_description] [varchar](200) NULL,
	[observation_date] [date] NULL,
	[inventory_location] [varchar](10) NULL,
	[inventory_location_name] [varchar](200) NULL,
	[inventory_location_category] [varchar](100) NULL,
	[location_code] [varchar](10) NULL,
	[product_category] [varchar](100) NULL,
	[product_type] [varchar](100) NULL,
	[available_to_promise_atp] [float] NULL,
	[stock_approaching_end_of_shelf_life_flag] [char](2) NULL,
	[end_of_shelf_life_flag] [char](2) NULL,
	[expiry_date] [date] NULL,
	[batch_number] [varchar](100) NULL,
	[merge_code] [varchar](100) NULL,
	[stock_level] [varchar](100) NULL,
	[region] [varchar](100) NULL,
	[subsidiary] [varchar](100) NULL,
	[inventory_in_transit] [float] NULL,
	[stock_on_hand] [float] NULL,
	[source_name] [varchar](100) NULL,
	[stock_status] [varchar](100) NULL,
	[stock_on_hand_value] [float] NULL,
	[stock_in_transit_value] [float] NULL,
	[commit_stock] [float] NULL,
	[product_life_cycle] [varchar](100) NULL,
	[latest_record] [char](2) NULL,
	[store_coverage] [float] NULL,
	[warehouse_coverage] [float] NULL,
	[safety_stock] [float] NULL,
	[min_level] [float] NULL,
	[max_level] [float] NULL,
	[forecast_period] [date] NULL,
	[forecast_date] [date] NULL,
	[Month_Number] [int] NULL,
	[week_of_year] [int] NULL,
	[year] [int] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO