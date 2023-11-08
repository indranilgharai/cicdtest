SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[data_profiling]
(
	[unique_id] [int] IDENTITY(1001,1) NOT NULL,
	[source_system_id] [varchar](100) NULL,
	[table_name] [varchar](200) NULL,
	[column_name] [varchar](200) NULL,
	[validity] [int] NULL,
	[uniqueness] [int] NULL,
	[completeness] [int] NULL,
	[record_details] [varchar](8000) NULL,
	[record_timestamp] [datetime] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO