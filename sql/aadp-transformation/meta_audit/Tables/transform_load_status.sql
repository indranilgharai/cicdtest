SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_audit].[transform_load_status]
(
	[LoadID] [int] IDENTITY(1,1) NOT NULL,
	[JobControlID] [int] NULL,
	[ZoneName] [varchar](30) NULL,
	[BatchID] [int] NULL,
	[JobID] [int] NULL,
	[LoadStatus] [varchar](20) NULL,
	[LatestExecutionFlag] [bit] NULL,
	[JobStartTime] [datetime] NULL,
	[JobEndTime] [datetime] NULL,
	[BatchStartTime] [datetime] NULL,
	[BatchEndTime] [datetime] NULL,
	[SPProcessingTime] [datetime] NULL,
	[DataDate] [datetime] NULL,
	[InputRowCount] [int] NULL,
	[TargetLoadCount] [int] NULL,
	[SourceStorageAccount] [varchar](50) NULL,
	[Source] [varchar](1000) NULL,
	[TargetStorageAccount] [varchar](50) NULL,
	[Target] [varchar](1000) NULL,
	[PipelineID] [varchar](100) NULL,
	[PipelineName] [varchar](200) NULL,
	[ErrorID] [int] NULL,
	[UserName] [varchar](200) NULL,
	[LastModifiedTime] [datetime] NULL,
	[LastWaterMark] [bigint] NULL,
	[ProjectName] [varchar](1000) NULL,
	[FileLastModifiedDate] [datetime] NULL,
	[FileTimeStamp] [numeric](18, 0) NULL,
	[metadatafiletotalname] [varchar](1000) NULL,
	[MetadataFileInputRowCount] [int] NULL,
	[MetadataFileTargetRowCount] [int] NULL,
	[MainFileArchived] [varchar](1000) NULL,
 CONSTRAINT [PK_transform_load_status] PRIMARY KEY NONCLUSTERED 
	(
		[LoadID] ASC
	) NOT ENFORCED 
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


