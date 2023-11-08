SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_class]
(
	[custrecord_ec_class_capex_appr_limit] [float] NULL,
	[custrecord_ec_class_capex_approver] [bigint] NULL,
	[custrecord_ec_class_code] [nvarchar](500) NULL,
	[custrecord_ec_class_id] [bigint] NULL,
	[custrecord_ec_class_opex_appr_limit] [float] NULL,
	[custrecord_ec_class_opex_approver] [bigint] NULL,
	[custrecord_ec_class_veritas_available] [nvarchar](500) NULL,
	[externalid] [nvarchar](500) NULL,
	[fullname] [nvarchar](500) NULL,
	[id] [bigint] NULL,
	[includechildren] [nvarchar](500) NULL,
	[isinactive] [nvarchar](500) NULL,
	[lastmodifieddate] [datetime2](7) NULL,
	[name] [nvarchar](500) NULL,
	[parent] [bigint] NULL,
	[subsidiary] [nvarchar](500) NULL,
	[md_record_written_timestamp] [nvarchar](500) NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [nvarchar](500) NULL
)
WITH
(
DISTRIBUTION = REPLICATE,
HEAP
)
GO