SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_currency_exchange_rate]
(
	[basecurrency] [bigint] NULL,
	[effectivedate] [datetime] NULL,
	[exchangerate] [float] NULL,
	[id] [bigint] NULL,
	[lastmodifieddate] [datetime] NULL,
	[transactioncurrency] [bigint] NULL,
	[md_record_written_timestamp] [nvarchar](500) NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [nvarchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO