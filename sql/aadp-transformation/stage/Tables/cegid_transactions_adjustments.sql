SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[cegid_transactions_adjustments]
(
	[header_correlationId] [nvarchar](max) NULL,
	[header_messageType] [nvarchar](max) NULL,
	[header_method] [nvarchar](max) NULL,
	[header_sourceSystem] [nvarchar](max) NULL,
	[body_filename] [nvarchar](max) NULL,
	[document_date] [nvarchar](max) NULL,
	[document_type] [nvarchar](max) NULL,
	[document_store] [nvarchar](max) NULL,
	[document_warehouse] [nvarchar](max) NULL,
	[document_number] [nvarchar](max) NULL,
	[document_internal_reference] [nvarchar](max) NULL,
	[line_number] [nvarchar](max) NULL,
	[item_code] [nvarchar](max) NULL,
	[quantity] [nvarchar](max) NULL,
	[reason_code] [nvarchar](max) NULL,
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