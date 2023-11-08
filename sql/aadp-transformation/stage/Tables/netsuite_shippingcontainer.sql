SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_shippingcontainer]
(
	[created] [datetime2](7) NULL,
	[custrecord_ec_sc_3pl_date_sent] [datetime2](7) NULL,
	[custrecord_ec_sc_arrival_date] [datetime2](7) NULL,
	[custrecord_ec_sc_bill_of_lading] [nvarchar](max) NULL,
	[custrecord_ec_sc_billing_address] [nvarchar](max) NULL,
	[custrecord_ec_sc_currency] [bigint] NULL,
	[custrecord_ec_sc_eta_date] [datetime2](7) NULL,
	[custrecord_ec_sc_from_address] [nvarchar](max) NULL,
	[custrecord_ec_sc_from_location] [bigint] NULL,
	[custrecord_ec_sc_from_subsidiary] [bigint] NULL,
	[custrecord_ec_sc_from_subsidiary_address] [nvarchar](max) NULL,
	[custrecord_ec_sc_no_of_cartons] [bigint] NULL,
	[custrecord_ec_sc_no_of_pallets] [bigint] NULL,
	[custrecord_ec_sc_order_date] [datetime2](7) NULL,
	[custrecord_ec_sc_printed_commercial_inv] [nvarchar](max) NULL,
	[custrecord_ec_sc_printed_packing_list] [nvarchar](max) NULL,
	[custrecord_ec_sc_seal_number] [nvarchar](max) NULL,
	[custrecord_ec_sc_send_to_3pl] [nvarchar](max) NULL,
	[custrecord_ec_sc_sent_to_3pl] [nvarchar](max) NULL,
	[custrecord_ec_sc_shipped_date] [datetime2](7) NULL,
	[custrecord_ec_sc_shipping_method] [bigint] NULL,
	[custrecord_ec_sc_status] [bigint] NULL,
	[custrecord_ec_sc_supplier] [bigint] NULL,
	[custrecord_ec_sc_terms] [bigint] NULL,
	[custrecord_ec_sc_to_address] [nvarchar](max) NULL,
	[custrecord_ec_sc_to_location] [bigint] NULL,
	[custrecord_ec_sc_to_subsidiary] [bigint] NULL,
	[custrecord_ec_sc_weight] [float] NULL,
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
