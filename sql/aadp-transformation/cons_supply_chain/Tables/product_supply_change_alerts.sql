/****** Modified Table [cons_supply_chain].[cons_product_supply_change_alerts] to accomodate higher  stock on hand values
    Modified Date: 7/01/2022 9:40:34 AM Modified by: Harsha Varadhi ******/

CREATE TABLE [cons_supply_chain].[product_supply_change_alerts]
(
	[sku] [varchar](100) NULL,
	[sku_description]  [varchar](100) NULL,
	[location]  [varchar](100) NULL,
	[inventory_location_code]  [varchar](100) NULL,
	[inventory_location_name] [varchar](100) NULL,
	[forecast_date] [date] NULL,
	[forecast_period] [date] NULL,
	[month_number] [varchar](100) NULL,
	[week_of_year] [varchar](100) NULL,
    [year] [varchar](100) NULL,
    [region] [varchar](100) NULL,
	[product_category] [varchar](100) NULL,
	[product_life_cycle] [varchar](100) NULL,
	[class] [varchar](100) NULL,
	[sub_class] [varchar](100) NULL,
	[supply_level] [varchar](100) NULL,
	[stock_on_hand_projected] decimal(12,4) NULL, /* Modified from 10 to 12 deciaml values to accomodate higher SOH values */
	[projected_safety_stock] decimal(12,4) NULL,
	[stock_coverage] decimal(12,4) NULL,
	[count_of_purchase_order] decimal(12,4) NULL,
	[inventory_in_transit] decimal(12,4) NULL,
	[planned_receipt] decimal(12,4) NULL,
	[planned_available] decimal(12,4) NULL,
	[shortage_quantity] float NULL,
	[shortage_time_duration] [varchar](100) NULL,
	[oos_instance] [varchar](100) NULL,
	[projected_service_level] float NULL,
	[demand_forecast] int NULL,
	[sku_formulation] [varchar](100) NULL,
	[stock_in_transit_value] decimal(12,4) NULL,
	[stock_on_hand_value] float NULL,
	[planned_week] float NULL,
	[shortage_value] float NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO