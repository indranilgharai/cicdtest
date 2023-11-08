/****** Object:  Table [stage].[netsuite_aggregateitemlocation_currentstandardcost]    Script Date: 12/14/2022 9:14:38 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_aggregateitemlocation_currentstandardcost]
(
	[item] [bigint] NULL,
	[location] [bigint] NULL, 
	[currentStandardCost] [float] NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max)
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO