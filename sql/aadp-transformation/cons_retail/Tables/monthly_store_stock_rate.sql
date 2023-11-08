/****** Object:  Table [cons_retail].[monthly_store_stock_rate]    Script Date: 1/26/2023 5:42:55 PM ******/
/****** Object:  Added new fields to the Table [cons_retail].[monthly_store_stock_rate]    Script Date: 7/27/2023 5:42:55 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[monthly_store_stock_rate]
(
	[dateKey] [varchar](100) NULL,
	[locationKey] [varchar](100) NULL,
	[mergeCode] [varchar](100) NULL,
	[date_loc_mergekey] [varchar](300) NULL,
	[thisMonth_total_sales_units] [int] NULL,
	[priorMonth_total_sales_units] [int] NULL,
	[last3Months_total_sales_units] [int] NULL,
	[last12Months_total_sales_units] [int] NULL,
	[thisMonth_total_sales_revAUD] [float] NULL,
	[priorMonth_total_sales_revAUD] [float] NULL,
	[last3Months_total_sales_revAUD] [float] NULL,
	[last12Months_total_sales_revAUD] [float] NULL,
	[days_inStock_Month] [int] NULL,
	[days_inStock_priorMonth] [int] NULL,
	[days_inStock_last3months] [int] NULL,
	[days_inStock_last12months] [int] NULL,
	[days_inStock_inTrade_Month] [int] NULL,
	[days_inStock_inTrade_priorMonth] [int] NULL,
	[days_inStock_inTrade_last3months] [int] NULL,
	[days_inStock__inTrade_last12months] [int] NULL,
	[days_outOfStock_Month] [int] NULL,
	[days_outOfStock_priorMonth] [int] NULL,
	[days_outOfStock_last3months] [int] NULL,
	[days_outOfStock_last12months] [int] NULL,
	[nDays_Month] [int] NULL,
	[nDays_priorMonth] [int] NULL,
	[nDays_last3months] [int] NULL,
	[nDays_last12months] [int] NULL,
	[nDays_storeTraded_Month] [int] NULL,
	[nDays_storeTraded_priorMonth] [int] NULL,
	[nDays_storeTraded_last3months] [int] NULL,
	[nDays_storeTraded_last12months] [int] NULL,
	[stock_on_hand_units_startMonth] [int] NULL,
	[stock_on_hand_value_startMonth] [float] NULL,
	[stock_on_hand_units_endMonth] [int] NULL,
	[stock_on_hand_value_endMonth] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(  DISTRIBUTION = HASH ( [date_loc_mergekey] ),
	CLUSTERED INDEX
	(
		[date_loc_mergekey] ASC
	)
)
GO
