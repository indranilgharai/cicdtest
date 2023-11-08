/****** Object:  Table [std].[itemcost_excluded_stock_list]    Script Date: 2/2/2023 1:44:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[itemcost_excluded_stock_list]
(
	[Store_no] [varchar](20) NULL,
	[Store] [varchar](50) NULL,
	[SKU] [varchar](50) NULL,
	[Description] [varchar](500) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
