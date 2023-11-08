SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[product_forecast]
(
	[sku_code] [varchar](100) NULL,
	[forecast_date] [date] NULL,
	[start_date] [date] NULL,
	[latest_forecast_flag] [varchar](100) NULL,
	[location_code] [varchar] (500) NULL,
	[channel_code] [varchar](100) NULL,
	[project_demand_units] [varchar](500) NULL,
	[cleared_actual_units] [varchar](500) NULL,
	[eff_from_dt] [datetime] NULL,
	[eff_to_dt] [varchar](500) NULL,
	[md_record_written_timestamp] [varchar](500) NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [nvarchar](200) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
