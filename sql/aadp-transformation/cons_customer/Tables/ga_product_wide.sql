
/****** Object:  Table [cons_customer].[ga_product_original]    Script Date: 5/20/2022 5:08:20 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_customer].[ga_product_wide]
(
	[visitNumber] [bigint] NULL,
	[visitId] [bigint] NULL,
	[visitStartTime] [bigint] NULL,
	[fullVisitorId] [varchar](200) NULL,
	[userId] [varchar](200) NULL,
	[clientId] [varchar](200) NULL,
	[date] [varchar](200) NULL,
	[hitNumber] [bigint] NULL,
	[transactionId] [varchar](200) NULL,
	[action_type] [varchar](200) NULL,
	[step] [bigint] NULL,
	[option] [nvarchar](200) NULL,
	[productSKU] [varchar](200) NULL,
	[v2ProductName] [nvarchar](200) NULL,
	[v2ProductCategory] [varchar](200) NULL,
	[productVariant] [nvarchar](200) NULL,
	[productBrand] [varchar](200) NULL,
	[productRevenue] [float] NULL,
	[localProductRevenue] [float] NULL,
	[productPrice] [bigint] NULL,
	[localProductPrice] [bigint] NULL,
	[productQuantity] [bigint] NULL,
	[productRefundAmount] [bigint] NULL,
	[localProductRefundAmount] [bigint] NULL,
	[isImpression] [varchar](10) NULL,
	[isClick] [varchar](10) NULL,
	[productListName] [nvarchar](200) NULL,
	[productListPosition] [bigint] NULL,
	[productCouponCode] [varchar](200) NULL,
	[cd_4] [varchar](200) NULL,
	[cd_5] [varchar](200) NULL,
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