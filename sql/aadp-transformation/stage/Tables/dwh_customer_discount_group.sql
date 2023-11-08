SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_customer_discount_group]
(
	[customer_code] [nvarchar](max) NULL,
	[price_lvl] [nvarchar](max) NULL,
	[customer_desc] [nvarchar](max) NULL,
	[discount_amt] [nvarchar](max) NULL,
	[country] [nvarchar](max) NULL,
	[country_iso_code] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
