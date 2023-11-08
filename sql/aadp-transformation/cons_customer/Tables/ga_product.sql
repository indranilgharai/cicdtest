/****** Object:  Table [cons_customer].[ga_product]    Script Date: 5/19/2022 10:32:58 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  TABLE [cons_customer].[ga_product]
(
	[concat_key] [varchar](600) NULL,
	[date] [date] null,
	[visitStartTime] [bigint] NULL,
	[fullVisitorId] [varchar](200) NULL,
	[VisitID] [varchar](200) NULL,
	[userId] [varchar](200) NULL,
	[transactionId] [varchar](200) NULL,
	[action_type] [varchar](200) NULL,
	[step] [bigint] NULL,
	[v2ProductCategory] [varchar](200) NULL,
	[productRevenue] [float] NULL,
	[productQuantity] [bigint] NULL,
	[cd_4] [varchar](200) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH (concat_key),
	CLUSTERED COLUMNSTORE INDEX
)
GO