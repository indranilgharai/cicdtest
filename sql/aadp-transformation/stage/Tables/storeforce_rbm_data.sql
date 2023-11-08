/****** Object:  Table [stage].[storeforce_rbm_data]    Script Date: 12/4/2022 10:59:26 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[storeforce_rbm_data]
(
	[Store Code] [nvarchar](max) NULL,
	[Store Name] [nvarchar](max) NULL,
	[Retail Business Code] [nvarchar](max) NULL,
	[Retail Business Name] [nvarchar](max) NULL,
	[Sub Region Code] [nvarchar](max) NULL,
	[Sub Region Name] [nvarchar](max) NULL,
	[Region Code] [nvarchar](max) NULL,
	[Region Name] [nvarchar](max) NULL,
	[Country] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


