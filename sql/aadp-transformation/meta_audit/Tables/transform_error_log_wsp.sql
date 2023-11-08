/****** Object:  Table [meta_audit].[transform_error_log_wsp]    Script Date: 2/21/2022 6:41:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_audit].[transform_error_log_wsp]
(
	[ErrorNumber] [varchar](500) NULL,
	[ErrorSeverity] [varchar](500) NULL,
	[ErrorState] [varchar](500) NULL,
	[ErrorProcedure] [varchar](500) NULL,
	[ErrorMessage] [varchar](4000) NULL,
	[ErrorJob] [int] NULL,
	[ErrorDate] [datetime] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


