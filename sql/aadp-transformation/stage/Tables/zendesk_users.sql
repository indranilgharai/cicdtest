/****** Object:  Table [stage].[zendesk_users]    Script Date: 4/29/2022 7:00:59 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[zendesk_users]
(
	[active] [nvarchar](max) NULL,
	[alias] [nvarchar](max) NULL,
	[created_at] [nvarchar](max) NULL,
	[custom_role_id] [nvarchar](max) NULL,
	[default_group_id] [nvarchar](max) NULL,
	[details] [nvarchar](max) NULL,
	[email] [nvarchar](max) NULL,
	[external_id] [nvarchar](max) NULL,
	[iana_time_zone] [nvarchar](max) NULL,
	[id] [nvarchar](max) NULL,
	[last_login_at] [nvarchar](max) NULL,
	[locale] [nvarchar](max) NULL,
	[locale_id] [nvarchar](max) NULL,
	[moderator] [nvarchar](max) NULL,
	[name] [nvarchar](max) NULL,
	[notes] [nvarchar](max) NULL,
	[only_private_comments] [nvarchar](max) NULL,
	[organization_id] [nvarchar](max) NULL,
	[permanently_deleted] [nvarchar](max) NULL,
	[phone] [nvarchar](max) NULL,
	[photo] [nvarchar](max) NULL,
	[report_csv] [nvarchar](max) NULL,
	[restricted_agent] [nvarchar](max) NULL,
	[role] [nvarchar](max) NULL,
	[role_type] [nvarchar](max) NULL,
	[shared] [nvarchar](max) NULL,
	[shared_agent] [nvarchar](max) NULL,
	[shared_phone_number] [nvarchar](max) NULL,
	[signature] [nvarchar](max) NULL,
	[suspended] [nvarchar](max) NULL,
	[tags] [nvarchar](max) NULL,
	[ticket_restriction] [nvarchar](max) NULL,
	[time_zone] [nvarchar](max) NULL,
	[two_factor_auth_enabled] [nvarchar](max) NULL,
	[updated_at] [nvarchar](max) NULL,
	[url] [nvarchar](max) NULL,
	[user_fields] [nvarchar](max) NULL,
	[verified] [nvarchar](max) NULL,
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


