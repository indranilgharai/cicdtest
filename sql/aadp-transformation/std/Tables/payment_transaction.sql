/****** Object:  Table [std].[payment_transaction]    Script Date: 3/24/2022 7:41:11 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[payment_transaction]
(
	[order_id] [varchar](100) NULL,
	[transaction_type] [varchar](100) NULL,
	[payment_method] [varchar](100) NULL,
	[amount] [float] NULL,
	[currency] [varchar](100) NULL,
	[payment_date] [datetimeoffset](7) NULL,
	[transaction_id] [varchar](100) NULL,
	[reference_id] [varchar](100) NULL,
	[payment_provider] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [order_id] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO


