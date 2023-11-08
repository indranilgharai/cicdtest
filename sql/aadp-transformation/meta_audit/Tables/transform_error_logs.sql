SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_audit].[transform_error_logs]
(
	[ErrorID] [int] IDENTITY(1,1) NOT NULL,
	[JobControlID] [int] NULL,
	[ZoneName] [varchar](30) NULL,
	[BatchID] [int] NULL,
	[JobID] [varchar](30) NULL,
	[PipelineID] [varchar](100) NULL,
	[ErrorType] [varchar](30) NULL,
	[ErrorCode] [varchar](50) NULL,
	[ErrorDescription] [varchar](8000) NULL,
	[ErrorLoggedTime] [datetime] NULL,
	[ProjectName] [varchar](1000) NULL,
	[metadatafiletotalname] [varchar](1000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


