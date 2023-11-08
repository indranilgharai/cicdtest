SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_shipping_container_detail]
(
	[amount] [float] NULL,
	[batches_received] [varchar](500) NULL,
	[batches_sent] [varchar](500) NULL,
	[batch_origin] [varchar](500) NULL,
	[completion_item_receipt] [varchar](500) NULL,
	[created_date] [datetime] NULL,
	[description] [varchar](500) NULL,
	[external_id] [varchar](500) NULL,
	[gross_amount] [varchar](500) NULL,
	[is_inactive] [varchar](500) NULL,
	[internal_id] [varchar](500) NULL,
	[item] [varchar](500) NULL,
	[item_fulfillment] [varchar](500) NULL,
	[item_receipt] [varchar](500) NULL,
	[last_modified_date] [datetime] NULL,
	[line_number] [varchar](500) NULL,
	[name] [varchar](500) NULL,
	[over_batches] [varchar](500) NULL,
	[over_quantity] [varchar](500) NULL,
	[over_transaction] [varchar](500) NULL,
	[owner] [varchar](500) NULL,
	[price] [varchar](500) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [item] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO