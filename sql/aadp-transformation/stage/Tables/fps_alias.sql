/****** Object:  Table [stage].[fps_alias]    Script Date: 2/23/2022 4:31:56 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[fps_alias]
(
	[Item_anonymised] [bit] NULL,
	[Item_created] [nvarchar](max) NULL,
	[Item_fps_created] [nvarchar](max) NULL,
	[Item_fps_last_modified] [nvarchar](max) NULL,
	[Item_hash_id] [nvarchar](max) NULL,
	[Item_person_uuid] [nvarchar](max) NULL,
	[Item_source] [nvarchar](max) NULL,
	[Item_source_id] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL,
	[Item_phone] [nvarchar](max) NULL,
	[Item_email] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
