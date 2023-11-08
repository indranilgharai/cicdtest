/****** Object:  Table [std].[DimAdjustment_Reasons]    Script Date: 12/14/2022 5:27:29 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[dimadjustment_reasons]
(
	[adjustment_code] [varchar](10) NULL,
	[adjustment_reason] [varchar](300) NULL,
	[adjustment_group] [varchar](100) NULL,
	[controllable_flag] [varchar](5) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
