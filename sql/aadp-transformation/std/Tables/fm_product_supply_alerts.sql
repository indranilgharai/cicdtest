SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[fm_product_supply_alerts]
(
	[sku_code] [varchar](200) NULL,
	[location_code] [varchar](200) NULL,
	[forecast_date] [date] NULL,
	[forecast_period] [varchar](200) NULL,
	[stock_on_hand] [varchar](200) NULL,
	[safety_stock] [varchar](200) NULL,
	[stock_coverage_no_of_weeks_for_soh] [varchar](200) NULL,
	[purchase_order] [varchar](200) NULL,
	[goods_in_transit] [varchar](200) NULL,
	[planned_receipt] [varchar](200) NULL,
	[planned_available] [varchar](200) NULL,
	[out_of_stock_flag] [varchar](25) NULL,
	[gross_requirement] [varchar](200) NULL,
	[week] [varchar](200) NULL,
	[startdateofweek] [varchar](200) NULL,
	[eff_from_dt] [date] NULL,
	[eff_to_dt] [date] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NOT NULL,
	[md_transformation_job_id] [varchar](500) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO