/****** Object:  Table [stage].[bq_ga_sessions_raw]    Script Date: 4/11/2022 7:36:43 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[bq_ga_sessions_raw]
(
	[visitorId] [bigint] NULL,
	[visitNumber] [bigint] NULL,
	[visitId] [bigint] NULL,
	[visitStartTime] [bigint] NULL,
	[fullVisitorId] [nvarchar](max) NULL,
	[userId] [nvarchar](max) NULL,
	[clientId] [nvarchar](max) NULL,
	[date] [nvarchar](max) NULL,
	[channelGrouping] [nvarchar](max) NULL,
	[socialEngagementType] [nvarchar](max) NULL,
	[visits] [bigint] NULL,
	[hits] [bigint] NULL,
	[pageviews] [bigint] NULL,
	[timeOnSite] [bigint] NULL,
	[bounces] [bigint] NULL,
	[transactions] [bigint] NULL,
	[transactionRevenue] [bigint] NULL,
	[newVisits] [bigint] NULL,
	[screenviews] [bigint] NULL,
	[uniqueScreenviews] [bigint] NULL,
	[sessionQualityDim] [bigint] NULL,
	[timeOnScreen] [bigint] NULL,
	[totalTransactionRevenue] [bigint] NULL,
	[referralPath] [nvarchar](max) NULL,
	[campaign] [nvarchar](max) NULL,
	[source] [nvarchar](max) NULL,
	[medium] [nvarchar](max) NULL,
	[keyword] [nvarchar](max) NULL,
	[adContent] [nvarchar](max) NULL,
	[isTrueDirect] [bit] NULL,
	[campaignCode] [nvarchar](max) NULL,
	[browser] [nvarchar](max) NULL,
	[operatingSystem] [nvarchar](max) NULL,
	[language] [nvarchar](max) NULL,
	[screenResolution] [nvarchar](max) NULL,
	[deviceCategory] [nvarchar](max) NULL,
	[continent] [nvarchar](max) NULL,
	[subContinent] [nvarchar](max) NULL,
	[country] [nvarchar](max) NULL,
	[region] [nvarchar](max) NULL,
	[metro] [nvarchar](max) NULL,
	[city] [nvarchar](max) NULL,
	[cityId] [nvarchar](max) NULL,
	[hitNumber] [bigint] NULL,
	[time] [bigint] NULL,
	[hour] [bigint] NULL,
	[minute] [bigint] NULL,
	[isInteraction] [bit] NULL,
	[isEntrance] [bit] NULL,
	[isExit] [bit] NULL,
	[referer] [nvarchar](max) NULL,
	[dataSource] [nvarchar](max) NULL,
	[pagePath] [nvarchar](max) NULL,
	[hostname] [nvarchar](max) NULL,
	[pageTitle] [nvarchar](max) NULL,
	[searchKeyword] [nvarchar](max) NULL,
	[searchCategory] [nvarchar](max) NULL,
	[pagePathLevel1] [nvarchar](max) NULL,
	[pagePathLevel2] [nvarchar](max) NULL,
	[pagePathLevel3] [nvarchar](max) NULL,
	[pagePathLevel4] [nvarchar](max) NULL,
	[transactionId] [nvarchar](max) NULL,
	[transactionRevenue_1] [bigint] NULL,
	[transactionTax] [bigint] NULL,
	[transactionShipping] [bigint] NULL,
	[affiliation] [nvarchar](max) NULL,
	[currencyCode] [nvarchar](max) NULL,
	[localTransactionRevenue] [bigint] NULL,
	[localTransactionTax] [bigint] NULL,
	[localTransactionShipping] [bigint] NULL,
	[transactionCoupon] [nvarchar](max) NULL,
	[eventCategory] [nvarchar](max) NULL,
	[eventAction] [nvarchar](max) NULL,
	[eventLabel] [nvarchar](max) NULL,
	[experimentId] [nvarchar](max) NULL,
	[experimentVariant] [nvarchar](max) NULL,
	[productSKU] [nvarchar](max) NULL,
	[v2ProductName] [nvarchar](max) NULL,
	[v2ProductCategory] [nvarchar](max) NULL,
	[productVariant] [nvarchar](max) NULL,
	[productBrand] [nvarchar](max) NULL,
	[productRevenue] [bigint] NULL,
	[localProductRevenue] [bigint] NULL,
	[productPrice] [bigint] NULL,
	[localProductPrice] [bigint] NULL,
	[productQuantity] [bigint] NULL,
	[productRefundAmount] [bigint] NULL,
	[localProductRefundAmount] [bigint] NULL,
	[isImpression] [bit] NULL,
	[isClick] [bit] NULL,
	[productListName] [nvarchar](max) NULL,
	[productListPosition] [bigint] NULL,
	[productCouponCode] [nvarchar](max) NULL,
	[action_type] [nvarchar](max) NULL,
	[step] [bigint] NULL,
	[option] [nvarchar](max) NULL,
	[socialInteractionNetwork] [nvarchar](max) NULL,
	[socialInteractionAction] [nvarchar](max) NULL,
	[socialInteractions] [bigint] NULL,
	[socialInteractionTarget] [nvarchar](max) NULL,
	[socialNetwork] [nvarchar](max) NULL,
	[uniqueSocialInteractions] [bigint] NULL,
	[hasSocialSourceReferral] [nvarchar](max) NULL,
	[socialInteractionNetworkAction] [nvarchar](max) NULL,
	[domainLookupTime] [bigint] NULL,
	[domContentLoadedTime] [bigint] NULL,
	[domInteractiveTime] [bigint] NULL,
	[domLatencyMetricsSample] [bigint] NULL,
	[pageDownloadTime] [bigint] NULL,
	[pageLoadSample] [bigint] NULL,
	[pageLoadTime] [bigint] NULL,
	[redirectionTime] [bigint] NULL,
	[serverConnectionTime] [bigint] NULL,
	[serverResponseTime] [bigint] NULL,
	[speedMetricsSample] [bigint] NULL,
	[userTimingCategory] [nvarchar](max) NULL,
	[userTimingLabel] [nvarchar](max) NULL,
	[userTimingSample] [bigint] NULL,
	[userTimingValue] [bigint] NULL,
	[userTimingVariable] [nvarchar](max) NULL,
	[promoCreative] [nvarchar](max) NULL,
	[promoId] [nvarchar](max) NULL,
	[promoName] [nvarchar](max) NULL,
	[promoPosition] [nvarchar](max) NULL,
	[type] [nvarchar](max) NULL,
	[contentGroup1] [nvarchar](max) NULL,
	[contentGroup2] [nvarchar](max) NULL,
	[contentGroup3] [nvarchar](max) NULL,
	[contentGroup4] [nvarchar](max) NULL,
	[contentGroup5] [nvarchar](max) NULL,
	[previousContentGroup1] [nvarchar](max) NULL,
	[previousContentGroup2] [nvarchar](max) NULL,
	[previousContentGroup3] [nvarchar](max) NULL,
	[previousContentGroup4] [nvarchar](max) NULL,
	[previousContentGroup5] [nvarchar](max) NULL,
	[contentGroupUniqueViews1] [bigint] NULL,
	[contentGroupUniqueViews2] [bigint] NULL,
	[contentGroupUniqueViews3] [bigint] NULL,
	[contentGroupUniqueViews4] [bigint] NULL,
	[contentGroupUniqueViews5] [bigint] NULL,
	[promoIsView] [bit] NULL,
	[promoIsClick] [bit] NULL,
	[cd_1] [nvarchar](max) NULL,
	[cd_2] [nvarchar](max) NULL,
	[cd_3] [nvarchar](max) NULL,
	[cd_4] [nvarchar](max) NULL,
	[cd_5] [nvarchar](max) NULL,
	[cd_6] [nvarchar](max) NULL,
	[cd_7] [nvarchar](max) NULL,
	[cd_8] [nvarchar](max) NULL,
	[cd_9] [nvarchar](max) NULL,
	[cd_10] [nvarchar](max) NULL,
	[cd_11] [nvarchar](max) NULL,
	[cd_12] [nvarchar](max) NULL,
	[cd_13] [nvarchar](max) NULL,
	[cd_14] [nvarchar](max) NULL,
	[cd_15] [nvarchar](max) NULL,
	[cd_16] [nvarchar](max) NULL,
	[cd_17] [nvarchar](max) NULL,
	[cd_18] [nvarchar](max) NULL,
	[cd_19] [nvarchar](max) NULL,
	[cd_20] [nvarchar](max) NULL,
	[cd_21] [nvarchar](max) NULL,
	[cd_22] [nvarchar](max) NULL,
	[cd_23] [nvarchar](max) NULL,
	[cd_24] [nvarchar](max) NULL,
	[cd_25] [nvarchar](max) NULL,
	[cd_26] [nvarchar](max) NULL,
	[cd_27] [nvarchar](max) NULL,
	[cd_28] [nvarchar](max) NULL,
	[cd_29] [nvarchar](max) NULL,
	[cd_30] [nvarchar](max) NULL,
	[cd_31] [nvarchar](max) NULL,
	[cd_32] [nvarchar](max) NULL,
	[cd_33] [nvarchar](max) NULL,
	[cd_34] [nvarchar](max) NULL,
	[cd_35] [nvarchar](max) NULL,
	[cd_36] [nvarchar](max) NULL,
	[cd_37] [nvarchar](max) NULL,
	[cd_38] [nvarchar](max) NULL,
	[cd_39] [nvarchar](max) NULL,
	[cd_40] [nvarchar](max) NULL,
	[cd_41] [nvarchar](max) NULL,
	[cd_42] [nvarchar](max) NULL,
	[cd_43] [nvarchar](max) NULL,
	[cd_44] [nvarchar](max) NULL,
	[cd_45] [nvarchar](max) NULL,
	[cd_46] [nvarchar](max) NULL,
	[cd_47] [nvarchar](max) NULL,
	[cd_48] [nvarchar](max) NULL,
	[cd_49] [nvarchar](max) NULL,
	[cd_50] [nvarchar](max) NULL,
	[cd_51] [nvarchar](max) NULL,
	[cd_52] [nvarchar](max) NULL,
	[cd_53] [nvarchar](max) NULL,
	[cd_54] [nvarchar](max) NULL,
	[cd_55] [nvarchar](max) NULL,
	[cd_56] [nvarchar](max) NULL,
	[cd_57] [nvarchar](max) NULL,
	[cd_58] [nvarchar](max) NULL,
	[cd_59] [nvarchar](max) NULL,
	[cd_60] [nvarchar](max) NULL,
	[cd_61] [nvarchar](max) NULL,
	[cd_62] [nvarchar](max) NULL,
	[cd_63] [nvarchar](max) NULL,
	[cd_64] [nvarchar](max) NULL,
	[cd_65] [nvarchar](max) NULL,
	[cd_66] [nvarchar](max) NULL,
	[cd_67] [nvarchar](max) NULL,
	[cd_68] [nvarchar](max) NULL,
	[cd_69] [nvarchar](max) NULL,
	[cd_70] [nvarchar](max) NULL,
	[cd_71] [nvarchar](max) NULL,
	[cd_72] [nvarchar](max) NULL,
	[cd_73] [nvarchar](max) NULL,
	[cd_74] [nvarchar](max) NULL,
	[cd_75] [nvarchar](max) NULL,
	[cd_76] [nvarchar](max) NULL,
	[cd_77] [nvarchar](max) NULL,
	[cd_78] [nvarchar](max) NULL,
	[cd_79] [nvarchar](max) NULL,
	[cd_80] [nvarchar](max) NULL,
	[cd_81] [nvarchar](max) NULL,
	[cd_82] [nvarchar](max) NULL,
	[cd_83] [nvarchar](max) NULL,
	[cd_84] [nvarchar](max) NULL,
	[cd_85] [nvarchar](max) NULL,
	[cd_86] [nvarchar](max) NULL,
	[cd_87] [nvarchar](max) NULL,
	[cd_88] [nvarchar](max) NULL,
	[cd_89] [nvarchar](max) NULL,
	[cd_90] [nvarchar](max) NULL,
	[cd_91] [nvarchar](max) NULL,
	[cd_92] [nvarchar](max) NULL,
	[cd_93] [nvarchar](max) NULL,
	[cd_94] [nvarchar](max) NULL,
	[cd_95] [nvarchar](max) NULL,
	[cd_96] [nvarchar](max) NULL,
	[cd_97] [nvarchar](max) NULL,
	[cd_98] [nvarchar](max) NULL,
	[cd_99] [nvarchar](max) NULL,
	[cd_100] [nvarchar](max) NULL,
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


