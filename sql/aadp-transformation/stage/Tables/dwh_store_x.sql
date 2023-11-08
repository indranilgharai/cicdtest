SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[dwh_store_x]
(
	[sbs_no] [int] NULL,
	[store_no] [int] NULL,
	[netsuite_location] [int] NULL,
	[store_name] [nvarchar](max) NULL,
	[pos_terminals] [int] NULL,
	[exclude] [nvarchar](max) NULL,
	[city] [nvarchar](max) NULL,
	[hub_city] [nvarchar](max) NULL,
	[state] [nvarchar](max) NULL,
	[sbs_region] [nvarchar](max) NULL,
	[address1] [nvarchar](max) NULL,
	[address2] [nvarchar](max) NULL,
	[postcode] [nvarchar](max) NULL,
	[phone] [nvarchar](max) NULL,
	[trading] [nvarchar](max) NULL,
	[trading_veritas] [nvarchar](max) NULL,
	[status] [nvarchar](max) NULL,
	[opening_date] [nvarchar](max) NULL,
	[closing_date] [nvarchar](max) NULL,
	[open_date] [datetime2](7) NULL,
	[close_date] [datetime2](7) NULL,
	[open_months] [int] NULL,
	[store_or_counter] [nvarchar](max) NULL,
	[channel] [nvarchar](max) NULL,
	[store_type] [nvarchar](max) NULL,
	[store_format] [nvarchar](max) NULL,
	[location_type] [nvarchar](max) NULL,
	[floor_space] [real] NULL,
	[tax_rate] [real] NULL,
	[total_floor_space] [real] NULL,
	[counter_type] [nvarchar](max) NULL,
	[mall_id] [nvarchar](max) NULL,
	[lfl] [nvarchar](max) NULL,
	[default_language] [nvarchar](max) NULL,
	[iso_default_language] [nvarchar](max) NULL,
	[location_code] [nvarchar](max) NULL,
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


