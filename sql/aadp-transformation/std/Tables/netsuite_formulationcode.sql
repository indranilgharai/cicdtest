SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_formulationcode]
(
	[created] [datetime] NULL,
	[custrecord_ec_formu_copy_message] [varchar](200) NULL,
	[custrecord_ec_formu_formulationversion] [varchar](200) NULL,
	[custrecord_ec_formu_manufacturingversion] [varchar](200) NULL,
	[custrecord_ec_mf_form_code_parent] [bigint] NULL,
	[custrecord_ec_mf_is_current] [varchar](200) NULL,
	[custrecord_ec_mf_location] [bigint] NULL,
	[custrecord_ec_mf_registered_code] [bigint] NULL,
	[externalid] [varchar](200) NULL,
	[id] [bigint] NULL,
	[isinactive] [varchar](200) NULL,
	[lastmodified] [datetime] NULL,
	[name] [varchar](200) NULL,
	[owner] [bigint]  NULL,
	[recordid] [bigint]  NULL,
	[scriptid] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NOT NULL,
	[md_transformation_job_id] [varchar](500) NOT NULL
)
WITH
(
DISTRIBUTION = REPLICATE,
HEAP
)
GO