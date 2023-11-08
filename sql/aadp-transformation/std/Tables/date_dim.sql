SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[date_dim]
(
	[counterval] [int] NULL,
	[date_id] [varchar](90) NULL,
	[incr_date] [date] NULL,
	[day_val] [int] NULL,
	[day_of_week] [int] NULL,
	[day_of_week_string] [nvarchar](30) NULL,
	[business_day] [varchar](3) NULL,
	[month_id] [int] NULL,
	[month_name] [nvarchar](30) NULL,
	[quarter_no] [int] NULL,
	[quarter_name] [varchar](31) NULL,
	[yearval] [int] NULL,
	[week_of_year] [int] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO