SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [meta_ctl].[transform_job_control]
(
	[JobControlID] [int] IDENTITY(9000,1) NOT NULL,
	[ZoneName] [varchar](20) NOT NULL,
	[BatchID] [int] NOT NULL,
	[JobID] [int] NOT NULL,
	[SourceType] [varchar](50) NULL,
	[SourceFormat] [varchar](100) NULL,
	[SourceFormatSubType] [varchar](100) NULL,
	[SourceStorageAccount] [varchar](50) NULL,
	[SourceName] [varchar](1500) NULL,
	[TargetFormat] [varchar](20) NULL,
	[TargetStorageAccount] [varchar](50) NULL,
	[TargetName] [varchar](1000) NULL,
	[EnabledFlag] [bit] NOT NULL,
	[Frequency] [varchar](50) NULL,
	[CreatedOn] [datetime] NULL,
	[CreatedBy] [varchar](50) NULL,
	[UpdatedOn] [datetime] NULL,
	[UpdatedBy] [varchar](50) NULL,
	[ArchivalLocation] [varchar](1000) NULL,
	[metadatafilename] [varchar](1000) NULL,
	[TargetPartitionFlag] [varchar](1000) NULL,
	[SourceStorageType] [varchar](100) NULL,
	[snapshot_flag] [int] NOT NULL,
	[dependent_job] [varchar](1000) NULL,
	[stage_number] [tinyint] NULL,
	[reset_flag] [bit] NULL,
 CONSTRAINT [PK_tranform_job_control] PRIMARY KEY NONCLUSTERED 
	(
		[ZoneName] ASC,
		[BatchID] ASC,
		[JobID] ASC
	) NOT ENFORCED 
)
WITH
(
	DISTRIBUTION = HASH ( [BatchID] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO

ALTER TABLE [meta_ctl].[transform_job_control] ADD  DEFAULT ((0)) FOR [reset_flag]
GO


