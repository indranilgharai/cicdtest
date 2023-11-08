/****** Object:  Table [stage].[cegid_order_header]    Script Date: 3/22/2022 6:59:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[cegid_order_header]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[active] [nvarchar](max) NULL,
	[cegid_order_id] [nvarchar](max) NULL,
	[total_items] [int] NULL,
	[total_price_value] [decimal](38, 18) NULL,
	[currency_iso] [nvarchar](max) NULL,
	[total_price_with_tax_value] [decimal](38, 18) NULL,
	[user_id] [nvarchar](max) NULL,
	[created] [nvarchar](max) NULL,
	[order_status] [nvarchar](max) NULL,
	[employee_id] [nvarchar](max) NULL,
	[created_time] [nvarchar](max) NULL,
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