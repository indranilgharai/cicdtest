/****** Object:  Table [meta_audit].[transform_job_stats]    Script Date: 2/21/2022 6:43:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_audit].[transform_job_stats]
(
	[job_id] [int] NULL,
	[job_start_time] [datetime] NULL,
	[job_end_time] [datetime] NULL,
	[job_status] [varchar](40) NULL,
	[log_message] [varchar](4000) NULL,
	[pipeline_id] [varchar](1000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


