/****** Object:  Table [meta_ctl].[transform_job_master]    Script Date: 2/21/2022 6:28:45 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_ctl].[transform_job_master]
(
	[job_id] [int] NULL,
	[job_name] [varchar](400) NULL,
	[description] [varchar](4000) NULL,
	[usecase_tag] [varchar](400) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


