SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[store_x]
(
	[sbs_no] [varchar](100) NULL,
	[store_no] [varchar](100) NULL,
	[netsuite_location] [varchar](100) NULL,
	[store_name] [varchar](100) NULL,
	[pos_terminals] [varchar](100) NULL,
	[exclude] [varchar](100) NULL,
	[city] [varchar](100) NULL,
	[hub_city] [varchar](100) NULL,
	[state] [varchar](100) NULL,
	[sbs_region] [varchar](100) NULL,
	[address1] [varchar](100) NULL,
	[address2] [varchar](100) NULL,
	[postcode] [varchar](100) NULL,
	[phone] [varchar](100) NULL,
	[trading] [varchar](100) NULL,
	[trading_veritas] [varchar](100) NULL,
	[status] [varchar](100) NULL,
	[opening_date] [varchar](100) NULL,
	[closing_date] [varchar](100) NULL,
	[open_date] [varchar](100) NULL,
	[close_date] [varchar](100) NULL,
	[open_months] [varchar](100) NULL,
	[store_or_counter] [varchar](100) NULL,
	[channel] [varchar](100) NULL,
	[store_type] [varchar](100) NULL,
	[store_format] [varchar](100) NULL,
	[location_type] [varchar](100) NULL,
	[floor_space] [varchar](100) NULL,
	[tax_rate] [varchar](100) NULL,
	[total_floor_space] [varchar](100) NULL,
	[counter_type] [varchar](100) NULL,
	[mall_id] [varchar](100) NULL,
	[lfl] [varchar](100) NULL,
	[default_language] [varchar](100) NULL,
	[iso_default_language] [varchar](100) NULL,
	[location_code] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
