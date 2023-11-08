/****** Object:  Table [stage].[dwh_cegid_online_seller]    Script Date: 7/25/2023 9:25:12 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_cegid_online_seller]
(

	[sbs_no] [smallint] NULL,
	[store_no] [int] NULL,
	[online_store_no] [int] NULL,
	[seller_code] [nvarchar](max) NULL,
	[seller_name] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL
)

WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO