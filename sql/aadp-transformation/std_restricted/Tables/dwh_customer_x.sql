SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std_restricted].[dwh_customer_x]
(
	[cust_sid] [varchar](100) NULL,
	[sbs_no] [int] NULL,
	[cust_id] [varchar](100) NULL,
	[first_name] [varchar](100) NULL,
	[last_name] [varchar](100) NULL,
	[phone_no] [nvarchar](max) NULL,
	[email_addr] [varchar](100) NULL,
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
