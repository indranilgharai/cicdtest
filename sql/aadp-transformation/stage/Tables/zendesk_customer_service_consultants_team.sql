/****** Object:  Table [stage].[zendesk_customer_service_consultants_team]    Script Date: 6/16/2023 7:20:16 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage].[zendesk_customer_service_consultants_team]
(
	[Assignee_tags] [nvarchar](max) NULL,
	[Assignee_name] [nvarchar](max) NULL,
	[Assignee_email] [nvarchar](max) NULL,
	[Team] [nvarchar](max) NULL,
	[Assignee_locale] [nvarchar](max) NULL,
	[Assignee_time_zone] [nvarchar](max) NULL,
	[Title] [nvarchar](max) NULL,
	[Assignee_status] [nvarchar](max) NULL,
	[Assignee_role] [nvarchar](max) NULL,
	[Requester_role] [nvarchar](max) NULL,
	[Assignee_ID] [nvarchar](max) NULL,
	[Assignee_sign_in_timestamp] [nvarchar](max) NULL,
	[Tickets] [nvarchar](max) NULL,
	[value_time_since assignee_login_min] [nvarchar](max) NULL,
	[Role] [nvarchar](max) NULL,
	[UserPrincipalEmail] [nvarchar](max) NULL,
	[ManagerID] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


