/****** Object:  Table [std].[dimitem_location]    Script Date: 1/24/2023 4:07:32 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[dimitem_location]
(
	[Sku_Code] [varchar](100) NULL,
	[locationID] [varchar](100) NULL,
	[locationid_skucode] [varchar](200) NULL,
	[cost] [float] NULL,
	[costingMethod] [varchar](100) NULL,
	[currentStandardCost] [float] NULL,
	[averageCost] [float] NULL,
	[lastpurchaseprice] [float] NULL,
	[excluded_from_stock] [varchar](100) NULL,
	[eff_from_date] [datetime] NULL,
	[eff_to_date] [datetime] NULL,
	[active_record] [int] NULL,
	[hash_value] [varbinary](8000) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(  DISTRIBUTION = HASH ( [locationid_skucode] ),
	CLUSTERED INDEX
	(
		[locationid_skucode] ASC
	)
	
)
GO


