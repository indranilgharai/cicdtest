SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[cegid_transactions_adjustments]
(
	[document_date] [varchar](100) NULL,
	[document_type] [varchar](100) NULL,
	[document_store] [varchar](10) NULL,
	[document_warehouse] [varchar](10) NULL,
	[document_number] [int] NULL,
	[document_internal_reference] [varchar](100) NULL,
	[line_number] [int] NULL,
	[item_code] [varchar](100) NULL,
	[quantity] [float] NULL,
	[reason_code] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [item_code] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO