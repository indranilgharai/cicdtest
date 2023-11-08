SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_class]
(
	[custrecord_ec_class_capex_appr_limit] [float] NULL,
	[custrecord_ec_class_capex_approver] [bigint] NULL,
	[custrecord_ec_class_code] [nvarchar](max) NULL,
	[custrecord_ec_class_id] [bigint] NULL,
	[custrecord_ec_class_opex_appr_limit] [float] NULL,
	[custrecord_ec_class_opex_approver] [bigint] NULL,
	[custrecord_ec_class_veritas_available] [nvarchar](max) NULL,
	[externalid] [nvarchar](max) NULL,
	[fullname] [nvarchar](max) NULL,
	[id] [bigint] NULL,
	[includechildren] [nvarchar](max) NULL,
	[isinactive] [nvarchar](max) NULL,
	[lastmodifieddate] [datetime2](7) NULL,
	[name] [nvarchar](max) NULL,
	[parent] [bigint] NULL,
	[subsidiary] [nvarchar](max) NULL,
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