--updated the column length for Billing_Address column from 200 to 500
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_shipping_container]
(
	[3PL_date_sent] [varchar](100) NULL,
	[arrival_date] [varchar](100) NULL,
	[billing_address] [varchar](500) NULL,
	[created] [datetime] NULL,
	[currency] [varchar](100) NULL,
	[eta_date] [datetime] NULL,
	[etd_shipped_date] [datetime] NULL,
	[external_id] [varchar](100) NULL,
	[from_address] [varchar](200) NULL,
	[from_location] [varchar](100) NULL,
	[from_subsidiary] [varchar](100) NULL,
	[from_subsidiary_address] [varchar](200) NULL,
	[is_inactive] [varchar](100) NULL,
	[internal_id] [varchar](100) NULL,
	[last_modified_date] [datetime] NULL,
	[name] [varchar](100) NULL,
	[no_of_cartons] [varchar](100) NULL,
	[no_of_pallets] [varchar](100) NULL,
	[order_date] [datetime] NULL,
	[owner] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](100) NULL,
	[md_transformation_job_id] [varchar](100) NULL,
	[md_source_system] [varchar](200) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [internal_id] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO