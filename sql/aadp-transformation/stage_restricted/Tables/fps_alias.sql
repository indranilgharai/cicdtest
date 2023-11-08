/****** Object:  Table [stage_restricted].[fps_alias]    Script Date: 3/31/2022 6:58:01 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage_restricted].[fps_alias]
(
	[Item_person_uuid] [nvarchar](max) NULL,
	[Item_phone] [nvarchar](max) NULL,
	[Item_email] [nvarchar](max) NULL,
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


