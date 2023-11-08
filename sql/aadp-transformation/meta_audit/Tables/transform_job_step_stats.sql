/****** Object:  Table [meta_audit].[transform_job_step_stats]    Script Date: 2/21/2022 6:45:47 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_audit].[transform_job_step_stats]
(
	[job_id] [int] NULL,
	[step_number] [int] NULL,
	[step_start_time] [datetime] NULL,
	[step_end_time] [datetime] NULL,
	[step_status] [varchar](40) NULL,
	[log_message] [varchar](4000) NULL,
	[driver_read_count] [int] NULL,
	[target_write_count] [int] NULL,
	[pipeline_id] [varchar](1000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


