SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[hybris_order_payments]
(
	[header_correlationId] [nvarchar](max) NULL,
	[header_messageType] [nvarchar](max) NULL,
	[header_method] [nvarchar](max) NULL,
	[header_sourceSystem] [nvarchar](max) NULL,
	[body_hybrisOrderId] [nvarchar](max) NULL,
	[payment_provider] [nvarchar](max) NULL,
	[payment_type] [nvarchar](max) NULL,
	[transaction_type] [nvarchar](max) NULL,
	[amount] [decimal](38, 18) NULL,
	[currency] [nvarchar](max) NULL,
	[payment_date] [nvarchar](max) NULL,
	[transaction_id] [nvarchar](max) NULL,
	[reference_id] [nvarchar](max) NULL,
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


