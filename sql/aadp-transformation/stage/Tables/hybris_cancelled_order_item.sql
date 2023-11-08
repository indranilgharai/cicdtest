/****** Object:  Table [stage].[hybris_cancelled_order_item]    Script Date: 3/22/2022 7:03:10 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[hybris_cancelled_order_item]
(
	[header_correlationId] [nvarchar](max) NULL,
	[header_messageType] [nvarchar](max) NULL,
	[header_method] [nvarchar](max) NULL,
	[header_sourceSystem] [nvarchar](max) NULL,
	[body_hybrisOrderId] [nvarchar](max) NULL,
	[entry_number] [int] NULL,
	[quantity_cancelled] [int] NULL,
	[cancelled_date] [nvarchar](max) NULL,
	[product_code] [nvarchar](max) NULL,
	[product_name] [nvarchar](max) NULL,
	[reason] [nvarchar](max) NULL,
	[reference_id] [nvarchar](max) NULL,
	[shipping_returned] [bit] NULL,
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