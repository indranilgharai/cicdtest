SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_currency]
(
	[currencyprecision] [bigint] NULL,
	[displaysymbol] [nvarchar](500) NULL,
	[exchangerate] [float] NULL,
	[externalid] [nvarchar](500) NULL,
	[fxrateupdatetimezone] [bigint] NULL,
	[id] [bigint] NULL,
	[includeinfxrateupdates] [nvarchar](500) NULL,
	[isbasecurrency] [nvarchar](500) NULL,
	[isinactive] [nvarchar](500) NULL,
	[lastmodifieddate] [datetime] NULL,
	[name] [nvarchar](500) NULL,
	[overridecurrencyformat] [nvarchar](500) NULL,
	[symbol] [nvarchar](500) NULL,
	[symbolplacement] [bigint] NULL,
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


