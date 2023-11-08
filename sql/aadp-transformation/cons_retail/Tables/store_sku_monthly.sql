/****** Object:  Table [cons_retail].[store_sku_monthly]    Script Date: 12/5/2022 8:45:11 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_retail].[store_sku_monthly]
(
	[storeskukey] [nvarchar](250) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
	[storekpikey] [varchar](250) NULL,
	[date_key] [date] NULL,
	[location_key] [varchar](10) NULL,
	[product_key] [varchar](50) NULL,
	[units] [int] NULL,
	[units_LY] [int] NULL,
	[revenue_in_aud] [float] NULL,
	[revenue_in_aud_LY] [float] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [storeskukey] ),
	CLUSTERED INDEX ( [storeskukey] )
)
GO


