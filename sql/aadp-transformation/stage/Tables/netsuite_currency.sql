SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_currency]
(
	[currencyprecision] [bigint] NULL,
	[displaysymbol] [nvarchar](max) NULL,
	[exchangerate] [float] NULL,
	[externalid] [nvarchar](max) NULL,
	[fxrateupdatetimezone] [bigint] NULL,
	[id] [bigint] NULL,
	[includeinfxrateupdates] [nvarchar](max) NULL,
	[isbasecurrency] [nvarchar](max) NULL,
	[isinactive] [nvarchar](max) NULL,
	[lastmodifieddate] [datetime2](7) NULL,
	[name] [nvarchar](max) NULL,
	[overridecurrencyformat] [nvarchar](max) NULL,
	[symbol] [nvarchar](max) NULL,
	[symbolplacement] [bigint] NULL,
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