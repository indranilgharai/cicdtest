
/****** Object:  Table [std].[sfmc_customer]    Script Date: 4/12/2022 6:18:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[sfmc_customer]
(
	[contactkey] [varchar](200) NULL,
	[svoc_email] [varchar](200) NULL,
	[optinemail] [varchar](200) NULL,
	[optinmobile] [varchar](200) NULL,
	[svocsource] [varchar](200) NULL,
	[rfv_class] [varchar](200) NULL,
	[rfv_segment_name] [varchar](200) NULL,
	[journeyname] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL
)
WITH
(
	DISTRIBUTION = hash(contactkey),
	CLUSTERED COLUMNSTORE INDEX
)
GO


