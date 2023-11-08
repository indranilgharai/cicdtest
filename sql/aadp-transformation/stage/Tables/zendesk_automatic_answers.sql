/****** Object:  Table [stage].[zendesk_automatic_answers]    Script Date: 4/29/2022 7:04:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[zendesk_automatic_answers]
(
	[answer_bot_channel] [nvarchar](max) NULL,
	[articles] [nvarchar](max) NULL,
	[brand_id] [nvarchar](max) NULL,
	[created_at] [nvarchar](max) NULL,
	[enquiry] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[solved_article_id] [nvarchar](max) NULL,
	[state] [nvarchar](max) NULL,
	[ticket_id] [nvarchar](max) NULL,
	[updated_at] [nvarchar](max) NULL,
	[user_id] [nvarchar](max) NULL,
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


