-- Modified Script [16/08/2022]:added new forecast flags that identifies latest records until 1, 2 and 3 months ago from today
-- Modified Script [16/08/2022]:modified forecast flags that identifies latest records until 1, 3 and 6 months ago from today
CREATE TABLE [cons_supply_chain].[product_forecast]
(
	[sku] [varchar](100) NULL,
	[sku_description]  [varchar](100) NULL,
	[forecast_date]  [datetime] NULL,
	[forecast_period]  [date] NULL,
	[Month_Number] [varchar](100) NULL,
	[year] [varchar](100) NULL,
	[latest_forecast_flag] [varchar](100) NULL,
	[region] [varchar](100) NULL,
	[subsidiary] [varchar](100) NULL,
    [location] [varchar](100) NULL,
    [channel_code] [varchar](100) NULL,
	[class] [varchar](100) NULL,
	[product_type_category] [varchar](100) NULL,
	[item_category] [varchar](100) NULL,
	[item_sub_category] [varchar](100) NULL,
	[product_life_cycle] [varchar](100) NULL,
	[projected_demand_units] int NULL,
	[actual_demand_units] int NULL,
	[projected_demand_value_AUD] float NULL,
	[actual_demand_value_AUD] float NULL,
	[projected_demand_value_local] float NULL,
	[actual_demand_value_local] float NULL,
	[percentage_accuracy] float NULL,
	[percentage_bias]  float NULL,
	[accuracy_target] float NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	-- added month-1,month-3,month-6 forecast flags
	[m_1_forecast_flag] [varchar](100) NULL,
	[m_3_forecast_flag] [varchar](100) NULL,
	[m_6_forecast_flag] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO