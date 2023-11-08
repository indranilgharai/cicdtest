/****** Object:  Table [std].[storeforce_rbm_data]    Script Date: 2/3/2023 6:43:33 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[storeforce_rbm_data]
(
	[Store_Code] [nvarchar](20) NULL,
	[Store_Name] [nvarchar](250) NULL,
	[Retail_Business_Code] [nvarchar](50) NULL,
	[Retail_Business_Name] [nvarchar](300) NULL,
	[Sub_Region_Code] [nvarchar](50) NULL,
	[Sub_Region_Name] [nvarchar](100) NULL,
	[Region_Code] [nvarchar](50) NULL,
	[Region_Name] [nvarchar](50) NULL,
	[Country] [nvarchar](10) NULL,
	[md_record_written_timestamp] [nvarchar](500) NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [nvarchar](500) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO
