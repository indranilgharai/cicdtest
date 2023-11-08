/****** Object:  Table [std].[customer_discount_group]    Script Date: 3/22/2022 7:05:08 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[customer_discount_group]
(
	[customer_code] [varchar](100) NULL,
	[price_lvl] [int] NULL,
	[customer_desc] [varchar](100) NULL,
	[discount_amt] [varchar](10) NULL,
	[country] [varchar](100) NULL,
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
