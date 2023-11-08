SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std_restricted].[dwh_rp_cust_address]
(
	[cust_sid] [varchar](100) NULL,
	[phone1] [varchar](100) NULL,
	[phone2] [varchar](100) NULL,
	[address1] [varchar](100) NULL,
	[address2] [varchar](100) NULL,
	[address3] [varchar](100) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
