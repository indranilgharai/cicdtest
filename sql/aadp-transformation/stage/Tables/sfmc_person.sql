SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[sfmc_person]
(
	[person_uuid] [nvarchar](max) NULL,
	[created] [nvarchar](max) NULL,
	[locale] [nvarchar](max) NULL,
	[title] [nvarchar](max) NULL,
	[skin_type] [nvarchar](max) NULL,
	[customer_group_id] [nvarchar](max) NULL,
	[last_surveyed] [nvarchar](max) NULL,
	[globalLanguage] [nvarchar](max) NULL,
	[optinEmail] [nvarchar](max) NULL,
	[optinEmailLastUpdatedDate] [nvarchar](max) NULL,
	[optinMobile] [nvarchar](max) NULL,
	[optinMobileLastUpdatedDate] [nvarchar](max) NULL,
	[pushOptin] [nvarchar](max) NULL,
	[pushOptinLastUpdatedDate] [nvarchar](max) NULL,
	[directMailOptin] [nvarchar](max) NULL,
	[directMailOptinLastUpdatedDate] [nvarchar](max) NULL,
	[telephoneMarketingOptin] [nvarchar](max) NULL,
	[telephoneMarketingOptinLastUpdatedDate] [nvarchar](max) NULL,
	[messagingAppOptin] [nvarchar](max) NULL,
	[messagingAppOptinLastUpdatedDate] [nvarchar](max) NULL,
	[emailBounceDate] [nvarchar](max) NULL,
	[mobileBounceDate] [nvarchar](max) NULL,
	[consentToThirdPartyMarketing] [nvarchar](max) NULL,
	[consentToShareDataAbroad] [nvarchar](max) NULL,
	[onlineAccountFlag] [nvarchar](max) NULL,
	[onlineAccountcreateDate] [nvarchar](max) NULL,
	[SubscriptionType] [nvarchar](max) NULL,
	[ml_updated_date] [nvarchar](max) NULL,
	[sfmc_inserted_cst_date] [nvarchar](max) NULL,
	[source] [nvarchar](max) NULL,
	[sfmc_updated_cst_date] [nvarchar](max) NULL,
	[accept_all_terms] [nvarchar](max) NULL,
	[EmailUnsubReason] [nvarchar](max) NULL,
	[ConsentToThirdPartyMarketingDate] [nvarchar](max) NULL,
	[ConsentToShareDataAbroadDate] [nvarchar](max) NULL,
	[globalCountry] [nvarchar](max) NULL,
	[Home_store] [nvarchar](max) NULL,
	[md_record_ingestion_timestamp] [nvarchar](max) NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](max) NULL,
	[md_source_system] [nvarchar](max) NULL,
	[mobileConnectLocale] [nvarchar](max) NULL,
	[email] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO