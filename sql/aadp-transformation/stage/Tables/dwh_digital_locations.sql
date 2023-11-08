/****** Object:  Table [stage].[dwh_digital_locations]    Script Date: 10/12/2023 5:57:23 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_digital_locations]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[warehouse_code] [nvarchar](max) NULL,
	[warehouse_code_location_code] [nvarchar](max) NULL,
	[virtual_warehouse_code] [nvarchar](max) NULL,
	[virtual_warehouse_code_location_code] [nvarchar](max) NULL,
	[shared_location] [int] NULL,
	[click_collect_cegid_online_location_code] [nvarchar](max) NULL,
	[active] [int] NULL,
	[inventory_flag] [int] NULL,
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
