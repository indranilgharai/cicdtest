/****** Object:  Table [cons_customer].[ga_session_original]    Script Date: 5/20/2022 5:10:13 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_customer].[ga_session_wide]
(
	[visitNumber] [bigint] NULL,
	[visitId] [bigint] NULL,
	[visitStartTime] [bigint] NULL,
	[fullVisitorId] [varchar](200) NULL,
	[userId] [varchar](200) NULL,
	[clientId] [varchar](200) NULL,
	[date] [varchar](200) NULL,
	[visits] [bigint] NULL,
	[hits] [bigint] NULL,
	[pageviews] [bigint] NULL,
	[timeOnSite] [bigint] NULL,
	[bounces] [bigint] NULL,
	[transactions] [bigint] NULL,
	[newVisits] [bigint] NULL,
	[screenviews] [bigint] NULL,
	[uniqueScreenviews] [bigint] NULL,
	[timeOnScreen] [bigint] NULL,
	[totalTransactionRevenue] [float] NULL,
	[referralPath] [varchar](4000) NULL,
	[campaign] [nvarchar](2500) NULL,
	[source] [nvarchar](2500) NULL,
	[medium] [nvarchar](2500) NULL,
	[keyword] [nvarchar](2500) NULL,
	[adContent] [nvarchar](2500) NULL,
	[isTrueDirect] [varchar](10) NULL,
	[campaignCode] [varchar](200) NULL,
	[browser] [varchar](200) NULL,
	[operatingSystem] [varchar](200) NULL,
	[language] [varchar](200) NULL,
	[screenResolution] [varchar](200) NULL,
	[deviceCategory] [varchar](200) NULL,
	[continent] [varchar](200) NULL,
	[country] [varchar](200) NULL,
	[region] [varchar](200) NULL,
	[city] [nvarchar](200) NULL,
	[cityId] [varchar](200) NULL,
	[channelGrouping] [varchar](200) NULL,
	[socialEngagementType] [varchar](200) NULL,
	[cd_1] [varchar](200) NULL,
	[cd_2] [varchar](200) NULL,
	[cd_6] [varchar](200) NULL,
	[cd_7] [varchar](200) NULL,
	[cd_8] [varchar](200) NULL,
	[cd_9] [varchar](200) NULL,
	[cd_13] [varchar](200) NULL,
	[cd_14] [varchar](200) NULL,
	[cd_17] [varchar](200) NULL,
	[cd_18] [varchar](200) NULL,
	[cd_20] [varchar](200) NULL,
	[cd_29] [varchar](200) NULL,
	[cd_31] [varchar](200) NULL,
	[cd_32] [varchar](200) NULL,
	[cd_33] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = hash(date),
	clustered columnstore index
)
GO