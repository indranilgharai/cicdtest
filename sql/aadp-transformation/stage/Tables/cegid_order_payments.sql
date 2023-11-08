SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[cegid_order_payments]
(
	[cegid_order_id] [nvarchar](max) NULL,
	[paymentTransactionId] [nvarchar](max) NULL,
	[amount] [decimal](38, 18) NULL,
	[currency] [nvarchar](max) NULL,
	[time] [nvarchar](max) NULL,
	[paymentType] [nvarchar](max) NULL,
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


