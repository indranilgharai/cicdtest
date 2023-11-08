-- Modified Script [16/08/2022]:added new forecast flags that identifies latest records until 1, 2 and 3 months ago from today
-- Modified Script [24/08/2022]:modified forecast flags that identifies latest records until 1, 3 and 6 months ago from today
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[fm_product_forecast]
(
	[sku_code] [varchar](100) NULL,
	[forecast_date] [date] NULL,
	[start_date] [date] NULL,
	[latest_forecast_flag] [varchar](100) NULL,
	[location_code] [varchar](500) NULL,
	[channel_code] [varchar](100) NULL,
	[project_demand_units] [int] NULL,
	[cleared_actual_units] [int] NULL,
	[eff_from_dt] [date] NULL,
	[eff_to_dt] [date] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [nvarchar](200) NULL,
	-- added month-1,month-3,month-6 forecast flags
	[m_1_forecast_flag] [varchar](100) NULL,
	[m_3_forecast_flag] [varchar](100) NULL,
	[m_6_forecast_flag] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED INDEX (sku_code,location_code,channel_code)
)
GO


