SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [std].[store_x]
(
	[sbs_no] [varchar](10) NULL,
	[store_no] [varchar](10) NULL,
	[netsuite_location] [varchar](10) NULL,
	[store_name] [varchar](100) NULL,
	[pos_terminals] [varchar](10) NULL,
	[exclude] [varchar](100) NULL,
	[city] [varchar](100) NULL,
	[hub_city] [varchar](100) NULL,
	[state] [varchar](100) NULL,
	[sbs_region] [varchar](100) NULL,
	[address1] [varchar](100) NULL,
	[address2] [varchar](100) NULL,
	[postcode] [varchar](100) NULL,
	[phone] [varchar](100) NULL,
	[trading] [varchar](10) NULL,
	[trading_veritas] [varchar](10) NULL,
	[status] [varchar](100) NULL,
	[opening_date] [varchar](100) NULL,
	[closing_date] [varchar](100) NULL,
	[open_date] [varchar](100) NULL,
	[close_date] [varchar](100) NULL,
	[open_months] [int] NULL,
	[store_or_counter] [varchar](100) NULL,
	[channel] [varchar](100) NULL,
	[store_type] [varchar](100) NULL,
	[store_format] [varchar](100) NULL,
	[location_type] [varchar](100) NULL,
	[floor_space] [float] NULL,
	[tax_rate] [float] NULL,
	[total_floor_space] [float] NULL,
	[counter_type] [varchar](100) NULL,
	[mall_id] [varchar](100) NULL,
	[lfl] [varchar](100) NULL,
	[default_language] [varchar](100) NULL,
	[iso_default_language] [varchar](100) NULL,
	[location_code] [varchar](10) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](30) NULL
)
WITH
(
	DISTRIBUTION = hash([location_code]),
	CLUSTERED COLUMNSTORE INDEX
)
GO