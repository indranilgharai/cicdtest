SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_productlifecycle]
(
	[altname] [nvarchar](max) NULL,
	[created] [datetime2](7) NULL,
	[custrecord_ec_plc_country_code] [nvarchar](max) NULL,
	[custrecord_ec_plc_date_available] [datetime2](7) NULL,
	[custrecord_ec_plc_date_discountinued] [datetime2](7) NULL,
	[custrecord_ec_plc_date_phaseout] [datetime2](7) NULL,
	[custrecord_ec_plc_item] [bigint] NULL,
	[custrecord_ec_plc_node_attribute] [nvarchar](max) NULL,
	[custrecord_ec_plc_obsolete_date] [datetime2](7) NULL,
	[custrecord_ec_plc_region] [bigint] NULL,
	[custrecord_ec_plc_status] [bigint] NULL,
	[externalid] [nvarchar](max) NULL,
	[id] [bigint] NULL,
	[isinactive] [nvarchar](max) NULL,
	[lastmodified] [datetime2](7) NULL,
	[name] [nvarchar](max) NULL,
	[owner] [bigint] NULL,
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


