/****** Object:  Table [std].[dimstocktake_schedule]    Script Date: 12/14/2022 5:26:08 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[dimstocktake_schedule]
(
	[locationkey] [varchar](20) NULL,
	[source_system] [varchar](20) NULL,
	[stocktake_name] [varchar](50) NULL,
	[stocktake_qtr] [varchar](5) NULL,
	[stocktake_year] [int] NULL,
	[stocktake_date] [date] NULL,
	[last_stocktake_name] [varchar](50) NULL,
	[last_stocktake_qtr] [varchar](5) NULL,
	[last_stocktake_year] [int] NULL,
	[last_stocktake_date] [date] NULL,
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
