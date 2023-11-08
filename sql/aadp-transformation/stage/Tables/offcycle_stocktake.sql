/****** Object:  Table [stage].[offcycle_stocktake]    Script Date: 12/14/2022 11:49:23 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[offcycle_stocktake]
(
	[Region] [nvarchar](max) NULL,
	[Subsidiary] [nvarchar](max) NULL,
	[Store_Name] [nvarchar](max) NULL,
	[POS] [nvarchar](max) NULL,
	[Reason for Off-Cycle Stocktake] [nvarchar](max) NULL,
	[Stocktake_Time (Local)] [nvarchar](max) NULL,
	[Stocktake_Time] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


