SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_currencyexchangerate]
(
	[basecurrency] [bigint] NULL,
	[effectivedate] [datetime2](7) NULL,
	[exchangerate] [float] NULL,
	[id] [bigint] NULL,
	[lastmodifieddate] [datetime2](7) NULL,
	[transactioncurrency] [bigint] NULL,
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