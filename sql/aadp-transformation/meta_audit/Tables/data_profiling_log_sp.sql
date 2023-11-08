SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_audit].[data_profiling_log_sp]
(
	[SP_Name] [varchar](500) NULL,
	[Detail] [varchar](4000) NULL,
	[SP_Status] [varchar](500) NULL,
	[Updated_Date] [datetime] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO