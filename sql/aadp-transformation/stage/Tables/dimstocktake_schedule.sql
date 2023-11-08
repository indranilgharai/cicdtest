/****** Object:  Table [stage].[dimstocktake_schedule]    Script Date: 12/14/2022 10:37:21 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dimstocktake_schedule]
(
	[locationkey] [nvarchar](max) NULL,
	[source_system] [nvarchar](max) NULL,
	[stocktake_name] [nvarchar](max) NULL,
	[stocktake_qtr] [nvarchar](max) NULL,
	[stocktake_year] [nvarchar](max) NULL,
	[stocktake_date] [nvarchar](max) NULL,
	[last_stocktake_name] [nvarchar](max) NULL,
	[last_stocktake_qtr] [nvarchar](max) NULL,
	[last_stocktake_year] [nvarchar](max) NULL,
	[last_stocktake_date] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
