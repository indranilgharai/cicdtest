/****** Object:  Table [std_restricted].[sfmc_svoc]    Script Date: 3/31/2022 7:08:26 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std_restricted].[sfmc_svoc]
(
	[contactkey] [varchar](100) NULL,
	[email] [varchar](100) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

