
/****** Object:  Table [std].[sfmc_svoc]    Script Date: 4/12/2022 7:53:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [std].[sfmc_svoc]
(
	[contactkey] [varchar](200) NULL,
	[email] [varchar](200) NULL,
	[mobileConnectLocale] [varchar](200) NULL,
	[locale] [varchar](200) NULL,
	[globalCountry] [varchar](200) NULL,
	[globalLanguage] [varchar](200) NULL,
	[title] [varchar](200) NULL,
	[skin_type] [varchar](200) NULL,
	[customer_group_id] [varchar](200) NULL,
	[last_surveyed] [datetime] NULL,
	[optinEmail] [varchar](200) NULL,
	[optinEmailLastUpdatedDate] [datetime] NULL,
	[emailBounceDate] [datetime] NULL,
	[EmailUnsubReason] [varchar](200) NULL,
	[optinMobile] [varchar](200) NULL,
	[optinMobileLastUpdatedDate] [datetime] NULL,
	[mobileBounceDate] [datetime] NULL,
	[pushOptin] [varchar](200) NULL,
	[pushOptinLastUpdatedDate] [datetime] NULL,
	[directMailOptin] [varchar](200) NULL,
	[directMailOptinLastUpdatedDate] [datetime] NULL,
	[telephoneMarketingOptin] [varchar](200) NULL,
	[telephoneMarketingOptinLastUpdatedDate] [datetime] NULL,
	[messagingAppOptin] [varchar](200) NULL,
	[messagingAppOptinLastUpdatedDate] [datetime] NULL,
	[consentToThirdPartyMarketing] [varchar](200) NULL,
	[ConsentToThirdPartyMarketingDate] [datetime] NULL,
	[consentToShareDataAbroad] [varchar](200) NULL,
	[ConsentToShareDataAbroadDate] [datetime] NULL,
	[accept_all_terms] [varchar](200) NULL,
	[created] [datetime] NULL,
	[source] [varchar](200) NULL,
	[SubscriptionType] [varchar](200) NULL,
	[onlineAccountFlag] [varchar](200) NULL,
	[onlineAccountcreateDate] [datetime] NULL,
	[first_purchase_channel] [varchar](200) NULL,
	[first_purchase_store] [varchar](200) NULL,
	[first_purchase_subsidiary] [varchar](200) NULL,
	[first_purchase_date] [datetime] NULL,
	[last_purchase_channel] [varchar](200) NULL,
	[last_purchase_store] [varchar](200) NULL,
	[last_purchase_subsidiary] [varchar](200) NULL,
	[last_purchase_date] [datetime] NULL,
	[lifetime_transactions] [bigint] NULL,
	[total_revenue_aud] [float] NULL,
	[most_purchased_subsidiary] [varchar](200) NULL,
	[second_purchase_date] [datetime] NULL,
	[random] [varchar](200) NULL,
	[RFV_Class] [varchar](200) NULL,
	[RFV_Segment_Name] [varchar](200) NULL,
	[EinsteinEmailEngagementPersona] [varchar](200) NULL,
	[InterestArray] [varchar](200) NULL,
	[Sent_180day] [bigint] NULL,
	[Unique_Opens_180day] [bigint] NULL,
	[Unique_Clicks_180day] [bigint] NULL,
	[Sent_90day] [bigint] NULL,
	[Unique_Opens_90day] [bigint] NULL,
	[Unique_Clicks_90day] [bigint] NULL,
	[Sent_30day] [bigint] NULL,
	[Unique_Opens_30day] [bigint] NULL,
	[Unique_Clicks_30day] [bigint] NULL,
	[sent_lifetime_count] [bigint] NULL,
	[open_lifetime_count] [bigint] NULL,
	[click_lifetime_count] [bigint] NULL,
	[Last_Email_Send_Date] [datetime] NULL,
	[Last_Email_Open_Date] [datetime] NULL,
	[Last_Email_Click_Date] [datetime] NULL,
	[Bronto_Open_Rate] [float] NULL,
	[Bronto_Click_Rate] [float] NULL,
	[Bronto_Last_Open_Date] [datetime] NULL,
	[Bronto_Last_Click_Date] [datetime] NULL,
	[Bronto_Status] [varchar](200) NULL,
	[first_txn_store_location_code] [varchar](200) NULL,
	[first_txn_store_name] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_ingestion_pipeline_id] [varchar](500) NULL,
	[md_source_system] [varchar](200) NULL,
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




