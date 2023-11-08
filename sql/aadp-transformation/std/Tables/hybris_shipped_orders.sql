SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[hybris_shipped_orders]
( 
	[order_id] [varchar](100)  NOT NULL,
	[shipped_date] [datetimeoffset](7)  NULL,
	[created] [datetimeoffset](7)  NULL,
	[md_record_ingestion_timestamp] [datetime]  NULL,
	[md_record_ingestion_pipeline_id] [varchar](500)  NULL,
	[md_source_system] [varchar](100)  NULL,
	[md_record_written_timestamp] [datetime]  NULL,
	[md_record_written_pipeline_id] [varchar](500)  NULL,
	[md_transformation_job_id] [varchar](500)  NULL
)
WITH
(
	DISTRIBUTION = HASH ( [order_id] ),
	CLUSTERED COLUMNSTORE INDEX
)