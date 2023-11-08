/****** Object:  Table [cons_customer].[ga_hits]    Script Date: 5/19/2022 10:28:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   TABLE [cons_customer].[ga_hits]
(
	[concat_key] [varchar](600) NULL,
	[concat_pagepath_key] [varchar](5000) NULL,
	[date] [date] null,
	[visitStartTime] [bigint] NULL,
	[fullVisitorId] [varchar](200) NULL,
	[VisitID] [varchar](200) NULL,
	[hitNumber] int NULL,
	[pageviews] [bigint] NULL,
	[timeOnSite] [bigint] NULL,
	[bounces] [bigint] NULL,
	[transactions] [bigint] NULL,
	[screenviews] [bigint] NULL,
	[uniqueScreenviews] [bigint] NULL,
	[timeOnScreen] [bigint] NULL,
	[referralPath] [varchar](4000) NULL,
	[keyword] [nvarchar](2500) NULL,
	[adContent] [nvarchar](2500) NULL,
	[browser] [varchar](200) NULL,
	[deviceCategory] [varchar](200) NULL,
	[country] [varchar](200) NULL,
	[region] [varchar](200) NULL,
	[type] [varchar](200) NULL,
	[time_on_page] [tinyint] NULL,
	[isEntrance] [tinyint] NULL,
	[isExit] [tinyint] NULL,
	[pagePath] [nvarchar](4000) NULL,
	[searchKeyword] [nvarchar](2500) NULL,
	[searchCategory] [varchar](200) NULL,
	[pagePathLevel1] [nvarchar](4000) NULL,
	[pagePathLevel4] [nvarchar](4000) NULL,
	[transactionId] [varchar](200) NULL,
	[eventCategory] [varchar](200) NULL,
	[eventAction] [nvarchar](4000) NULL,
	[eventLabel] [nvarchar](2500) NULL,
	[action_type] [varchar](200) NULL,
	[step] [bigint] NULL,
	[channelGrouping] [varchar](200) NULL,
	[contentGroup1] [varchar](200) NULL,
	[contentGroup2] [varchar](200) NULL,
	[cd_1] [varchar](200) NULL,
	[cd_2] [varchar](200) NULL,
	[cd_4] [varchar](200) NULL,
	[cd_5] [varchar](200) NULL,
	[cd_7] [varchar](200) NULL,
	[cd_13] [varchar](200) NULL,
	[cd_14] [varchar](200) NULL,
	[cd_16] [nvarchar](200) NULL,
	[cd_17] [varchar](200) NULL,
	[cd_18] [varchar](200) NULL,
	[cd_20] [varchar](200) NULL,
	[cd_22] [varchar](200) NULL,
	[cd_23] [varchar](200) NULL,
	[cd_24] [varchar](200) NULL,
	[cd_25] [nvarchar](200) NULL,
	[cd_26] [varchar](200) NULL,
	[cd_27] [varchar](200) NULL,
	[cd_28] [varchar](200) NULL,
	[cd_29] [varchar](200) NULL,
	[cd_31] [varchar](200) NULL,
	[cd_32] [varchar](200) NULL,
	[cd_33] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH (concat_key),
	CLUSTERED COLUMNSTORE INDEX
)
GO