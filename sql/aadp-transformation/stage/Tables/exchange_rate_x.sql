SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[exchange_rate_x]
(
	[sbs_no] [nvarchar](max) NULL,
	[month_no] [nvarchar](max) NULL,
	[year] [nvarchar](max) NULL,
	[fy] [nvarchar](max) NULL,
	[ex_rate] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO