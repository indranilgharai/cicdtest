SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_inventory_in_transit]
(
	[created_date] [datetime] NULL,
	[amount] [float] NULL,
	[batch_detail] [varchar](500) NULL,
	--Updated value from varchar(1500) to varchar(2500)
	[batch_detail_received] [varchar](2500) NULL,
	[container_detail] [varchar](1500) NULL,
	[container_received] [varchar](1500) NULL,
	[fulfillment_date] [datetime] NULL,
	[intercompany] [char](2) NULL,
	[item] [varchar](200) NULL,
	[item_fulfillment] [varchar](200) NULL,
	[last_receipt_date] [datetime] NULL,
	[iit_order] [varchar](200) NULL,
	[partial_receipts] [varchar](2500) NULL,
	[transit_quantity] [float] NULL,
	[subsidiary] [varchar](200) NULL,
	[external_id] [varchar](200) NULL,
	[id] [varchar](200) NULL,
	[is_inactive] [char](2) NULL,
	[last_modified] [datetime] NULL,
	[name] [varchar](500) NULL,
	[owner_employee] [varchar](200) NULL,
	[record_id] [varchar](200) NULL,
	[script_id] [varchar](200) NULL,
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


