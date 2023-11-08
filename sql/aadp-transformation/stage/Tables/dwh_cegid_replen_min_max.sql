SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_cegid_replen_min_max]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[description1] [nvarchar](max) NULL,
	[min_old] [int] NULL,
	[min_new] [int] NULL,
	[max_old] [int] NULL,
	[max_new] [int] NULL,
	[created_user] [nvarchar](max) NULL,
	[created_date] [nvarchar](max) NULL,
	[export_user] [nvarchar](max) NULL,
	[export_date] [nvarchar](max) NULL,
	[export_status] [nvarchar](max) NULL,
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