SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[netsuite_item_inventory]
(
	[store_warehouse_code] [varchar](100) NULL,
	[item_code] [varchar](100) NULL,
	[physical_inventory] [float] NULL,
	[qty_reserved] [float] NULL,
	[qty_available] [float] NULL,
	[qty_in_transit_warehouse] [float] NULL,
	[qty_in_transit_store] [float] NULL,
	[soh_time_stamp] [date] NULL,
	[channel] [varchar](100) NULL,
	[source] [varchar](100) NULL,
	[inventory_number] [varchar](100) NULL,
	[expiry_date] [date] NULL,
	[last_modified_date] [datetime] NULL,
	[stock_status] [varchar](100) NULL,
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