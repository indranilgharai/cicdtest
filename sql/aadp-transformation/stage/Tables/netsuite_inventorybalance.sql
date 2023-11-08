/****** Object:  Table [stage].[netsuite_inventorybalance]    Script Date: 7/19/2022 1:03:58 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[netsuite_inventorybalance] 
(
	[quantityavailable] [float] NULL,
	[binnumber] [bigint] NULL,
	[committedqtyperlocation] [float] NULL,
  [committedqtyperseriallotnumber] [float] NULL,
	[committedqtyperseriallotnumberlocation] [float] NULL,
	[item] [bigint] NULL,
	[location] [bigint] NULL,
	[quantityonhand] [float] NULL,
	[quantitypicked] [float] NULL,
	[inventorynumber] [bigint] NULL,
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
