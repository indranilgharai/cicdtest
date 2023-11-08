/****** Object:  Table [stage].[dimadjustment_reasons]    Script Date: 12/14/2022 11:16:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dimadjustment_reasons]
(
	[Reason_Code] [nvarchar](max) NULL,
	[Reason_Code_Description] [nvarchar](max) NULL,
	[Adjustment_Group] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
