SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_store_sku_daily_sales_rate]
(
	[store_location_code] [varchar](200) NULL,
	[item_code] [varchar](200) NULL,
	[sales_units] [float] NULL,
	[daily_sales_rate] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [item_code] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO