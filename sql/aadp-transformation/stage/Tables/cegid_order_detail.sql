/****** Object:  Table [stage].[cegid_order_detail]    Script Date: 3/22/2022 6:59:46 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[cegid_order_detail]
(
	[cegid_order_id] [nvarchar](max) NULL,
	[entry_number] [int] NULL,
	[quantity] [int] NULL,
	[product_code] [nvarchar](max) NULL,
	[product_name] [nvarchar](max) NULL,
	[total_price_no_tax] [decimal](38, 18) NULL,
	[currency_iso] [nvarchar](max) NULL,
	[total_tax] [decimal](38, 18) NULL,
	[correlation_id] [nvarchar](max) NULL,
	[message_type] [nvarchar](max) NULL,
	[method] [nvarchar](max) NULL,
	[source_system] [nvarchar](max) NULL,
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