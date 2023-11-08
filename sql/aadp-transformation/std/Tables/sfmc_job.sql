
/****** Object:  Table [std].[sfmc_job]    Script Date: 4/12/2022 7:06:46 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[sfmc_job]
(
	[HashKey] [varchar](200) NULL,
	[JobID] [bigint] NULL,
	[EmailID] [bigint] NULL,
	[AccountID] [bigint] NULL,
	[AccountUserID] [bigint] NULL,
	[FromName] [varchar](200) NULL,
	[FromEmail] [varchar](200) NULL,
	[SchedTime] [datetime] NULL,
	[PickupTime] [datetime] NULL,
	[DeliveredTime] [datetime] NULL,
	[EventID] [varchar](30) NULL,
	[IsMultipart] [varchar](30) NULL,
	[JobType] [varchar](200) NULL,
	[JobStatus] [varchar](200) NULL,
	[ModifiedBy] [bigint] NULL,
	[ModifiedDate] [datetime] NULL,
	[EmailName] [varchar](200) NULL,
	[EmailSubject] [varchar](200) NULL,
	[IsWrapped] [varchar](200) NULL,
	[TestEmailAddr] [varchar](200) NULL,
	[Category] [varchar](200) NULL,
	[BccEmail] [varchar](200) NULL,
	[OriginalSchedTime] [datetime] NULL,
	[CreatedDate] [datetime] NULL,
	[CharacterSet] [varchar](200) NULL,
	[IPAddress] [varchar](200) NULL,
	[SalesForceTotalSubscriberCount] [bigint] NULL,
	[SalesForceErrorSubscriberCount] [bigint] NULL,
	[SendType] [varchar](200) NULL,
	[DynamicEmailSubject] [varchar](200) NULL,
	[SuppressTracking] [varchar](200) NULL,
	[SendClassificationType] [varchar](200) NULL,
	[SendClassification] [varchar](200) NULL,
	[ResolveLinksWithCurrentData] [varchar](200) NULL,
	[EmailSendDefinition] [varchar](200) NULL,
	[DeduplicateByEmail] [varchar](200) NULL,
	[TriggererSendDefinitionObjectID] [varchar](200) NULL,
	[TriggeredSendCustomerKey] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NOT NULL,
	[md_transformation_job_id] [varchar](500) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


