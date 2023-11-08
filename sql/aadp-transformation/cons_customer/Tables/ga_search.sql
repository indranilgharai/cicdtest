/****** Object:  Table [cons_customer].[ga_search]    Script Date: 5/12/2022 2:10:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_customer].[ga_search]
(
	[date] [date] NULL,
	[fullVisitorId] [varchar](200) NULL,
	[VisitID] [varchar](200) NULL,
	[visitStartTime] [bigint] NULL,
	[clientid] [varchar](200) NULL,
	[searchterm] [nvarchar](2500) NULL,
	[hashkey] [varbinary](8000) NULL,
	[results_pageviews] [int] NULL,
	[search_exits] [int] NULL,
	[search_refinements] [int] NULL,
	[time_after_search] [float] NULL,
	[search_depth] [int] NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NOT NULL,
	[md_transformation_job_id] [varchar](500) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO