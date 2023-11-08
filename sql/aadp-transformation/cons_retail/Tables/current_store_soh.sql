/****** Object:  Table [cons_retail].[current_store_soh]    Script Date: 1/10/2023 12:58:28 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[current_store_soh]
(
	[locationKey] [varchar](100) NOT NULL,
	[skuKey] [varchar](100) NOT NULL,
	[location_skukey] [varchar](200) NOT NULL,
	[stock_on_hand_units] [float] NULL,
	[stock_on_hand_value] [float] NULL,
	[stock_in_transit_units] [float] NULL,
	[stock_in_transit_value] [float] NULL,
	[vm_minimum_units] [int] NULL,
	[vm_minimum_value] [float] NULL,
	[sellable_soh_units] [float] NULL,
	[sellable_soh_value] [float] NULL,
	[cegid_min] [int] NULL,
	[cegid_max] [int] NULL,
	[expected_next_order_packs] [int] NULL,
	[expected_next_order_units] [int] NULL,
	[inStock_trading_28days_sales_units] [int] NULL,
	[ndays_identified] [int] NULL,
	[daily_sales_rate] [float] NULL,
	[snapshot_date] [date] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(  DISTRIBUTION = HASH ( [location_skukey] ),
	CLUSTERED INDEX
	(
		[location_skukey] ASC
	)
)
GO
