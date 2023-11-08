/****** Object:  Table [cons_customer].[ga_session]    Script Date: 5/19/2022 10:34:18 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [cons_customer].[ga_session]
(	
	[concat_key] [varchar](600) NULL,
	[visitStartTime] [bigint] NULL,
	[fullVisitorId] [varchar](200) NULL,
	[VisitID] [varchar](200) NULL,
	[date] [date] NULL,
	search_flag tinyint null,
	[pageviews] [bigint] NULL,
	[timeOnSite] [bigint] NULL,
	[bounces] [bigint] NULL,
	[transactions] [bigint] NULL,
	[screenviews] [bigint] NULL,
	[uniqueScreenviews] [bigint] NULL,
	[timeOnScreen] [bigint] NULL,
	[totalTransactionRevenue] [float] NULL,
	[referralPath] [varchar](4000) NULL,
	[source] [nvarchar](2500) NULL,
	[medium] [nvarchar](2500) NULL,
	[keyword] [nvarchar](2500) NULL,
	[browser] [varchar](200) NULL,
	[deviceCategory] [varchar](200) NULL,
	[country] [varchar](200) NULL,
	[region] [varchar](200) NULL,
	[channelGrouping] [varchar](200) NULL,
	[cd_1] [varchar](200) NULL,
	[cd_2] [varchar](200) NULL,
	[cd_6] [varchar](200) NULL,
	[cd_7] [varchar](200) NULL,
	[cd_13] [varchar](200) NULL,
	[cd_14] [varchar](200) NULL,
	[cd_20] [varchar](200) NULL,
	[cd_33] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH ( concat_key ),
	CLUSTERED COLUMNSTORE INDEX
)
GO