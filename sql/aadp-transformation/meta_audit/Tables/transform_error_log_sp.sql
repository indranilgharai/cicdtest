/****** Object:  Table [meta_audit].[transform_error_log_sp]    Script Date: 2/21/2022 6:33:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_audit].[transform_error_log_sp]
(
	[ErrorNumber] [int] NULL,
	[ErrorSeverity] [int] NULL,
	[ErrorState] [int] NULL,
	[ErrorProcedure] [varchar](500) NULL,
	[ErrorMessage] [varchar](4000) NULL,
	[updated_date] [datetime] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
