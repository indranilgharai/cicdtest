SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_inventoryintransit]
(
	[created] [datetime2](7) NULL,
	[custrecord_ec_iit_amount] [float] NULL,
	[custrecord_ec_iit_batchdetail] [nvarchar](max) NULL,
	[custrecord_ec_iit_batchrcvd] [nvarchar](max) NULL,
	[custrecord_ec_iit_container] [nvarchar](max) NULL,
	[custrecord_ec_iit_containerrcvd] [nvarchar](max) NULL,
	[custrecord_ec_iit_fulfillment_date] [datetime2](7) NULL,
	[custrecord_ec_iit_intercompany] [nvarchar](max) NULL,
	[custrecord_ec_iit_item] [bigint] NULL,
	[custrecord_ec_iit_item_fulfillment] [bigint] NULL,
	[custrecord_ec_iit_last_receipt_date] [datetime2](7) NULL,
	[custrecord_ec_iit_order] [bigint] NULL,
	[custrecord_ec_iit_partial_receipts] [nvarchar](max) NULL,
	[custrecord_ec_iit_quantity] [float] NULL,
	[custrecord_ec_iit_subsidiary] [bigint] NULL,
	[externalid] [nvarchar](max) NULL,
	[id] [bigint] NULL,
	[isinactive] [nvarchar](max) NULL,
	[lastmodified] [datetime2](7) NULL,
	[name] [nvarchar](max) NULL,
	[owner] [bigint] NULL,
	[recordid] [bigint] NULL,
	[scriptid] [nvarchar](max) NULL,
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


