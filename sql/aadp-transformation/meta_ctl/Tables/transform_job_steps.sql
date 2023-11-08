/****** Object:  Table [meta_ctl].[transform_job_steps]    Script Date: 2/21/2022 6:30:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_ctl].[transform_job_steps]
(
	[job_id] [int] NULL,
	[step_number] [int] NULL,
	[sp_name] [varchar](400) NULL,
	[sp_parms] [varchar](400) NULL,
	[dependent_step_num] [varchar](400) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


