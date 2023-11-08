
/****** Object:  Table [meta_ctl].[transform_count_record_table]    Script Date: 2/21/2022 6:23:40 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_ctl].[transform_count_record_table]
(
	[job_id] [int] NULL,
	[step_number] [int] NULL,
	[driver_read_count] [int] NULL,
	[target_write_count] [int] NULL,
	[md_record_written_timestamp] [datetime] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


