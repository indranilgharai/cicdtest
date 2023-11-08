/****** Object:  Table [stage].[aggregateitemlocation]    Script Date: 8/16/2022 7:05:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_aggregateitemlocation]
(
	[item] [bigint] NULL,
	[location] [bigint] NULL, 
	[quantityavailable] [float] NULL,
	[quantityintransit] [float] NULL,
	[quantityonhand] [float] NULL,
	[quantityonorder] [float] NULL,
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