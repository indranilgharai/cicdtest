/****** Object:  Table [cons_retail].[Store_Inventory_Adjustments]    Script Date: 12/14/2022 5:29:07 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[store_inventory_adjustments]
(
	[dateKey] [date] NULL,
	[locationKey] [varchar](10) NULL,
	[productKey] [varchar](30) NULL,
	[location_productKey] [varchar](50) NULL,
	[adjustment_code] [varchar](5) NULL,
	[adjustment_reason] [varchar](500) NULL,
	[adjustment_units] [int] NULL,
	[item_cost_aud] [float] NULL,
	[item_rrp_aud] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
