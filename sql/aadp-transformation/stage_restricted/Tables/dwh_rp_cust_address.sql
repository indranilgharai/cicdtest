SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage_restricted].[dwh_rp_cust_address]
(
	[cust_sid] [nvarchar](max) NULL,
	[phone1] [nvarchar](max) NULL,
	[phone2] [nvarchar](max) NULL,
	[address1] [nvarchar](max) NULL,
	[address2] [nvarchar](max) NULL,
	[address3] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
