

/****** Object:  Table [std].[sfmc_person]    Script Date: 4/12/2022 7:24:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [std].[sfmc_person]
(
	[person_uuid] [varchar](200) NULL,
	[created] [datetime] NULL,
	[locale] [varchar](200) NULL,
	[title] [varchar](200) NULL,
	[skin_type] [varchar](200) NULL,
	[customer_group_id] [varchar](200) NULL,
	[last_surveyed] [datetime] NULL,
	[globalLanguage] [varchar](200) NULL,
	[optinEmail] [varchar](200) NULL,
	[optinEmailLastUpdatedDate] [datetime] NULL,
	[optinMobile] [varchar](200) NULL,
	[optinMobileLastUpdatedDate] [datetime] NULL,
	[pushOptin] [varchar](200) NULL,
	[pushOptinLastUpdatedDate] [datetime] NULL,
	[directMailOptin] [varchar](200) NULL,
	[directMailOptinLastUpdatedDate] [datetime] NULL,
	[telephoneMarketingOptin] [varchar](200) NULL,
	[telephoneMarketingOptinLastUpdatedDate] [datetime] NULL,
	[messagingAppOptin] [varchar](200) NULL,
	[messagingAppOptinLastUpdatedDate] [datetime] NULL,
	[emailBounceDate] [datetime] NULL,
	[mobileBounceDate] [datetime] NULL,
	[consentToThirdPartyMarketing] [varchar](200) NULL,
	[consentToShareDataAbroad] [varchar](200) NULL,
	[onlineAccountFlag] [varchar](200) NULL,
	[onlineAccountcreateDate] [datetime] NULL,
	[SubscriptionType] [varchar](200) NULL,
	[ml_updated_date] [datetime] NULL,
	[sfmc_inserted_cst_date] [datetime] NULL,
	[source] [varchar](200) NULL,
	[sfmc_updated_cst_date] [datetime] NULL,
	[accept_all_terms] [varchar](200) NULL,
	[EmailUnsubReason] [varchar](200) NULL,
	[ConsentToThirdPartyMarketingDate] [datetime] NULL,
	[ConsentToShareDataAbroadDate] [datetime] NULL,
	[globalCountry] [varchar](200) NULL,
	[Home_store] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
	[mobileConnectLocale] [varchar](200) NULL,
	[email] [varchar](200) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


