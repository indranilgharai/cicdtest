SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stage].[zendesk_contact_reason_mapping]
(
	[ticket_form_id] [nvarchar](max) NULL,
	[ticket_form_description] [nvarchar](max) NULL,
	[contact_reason] [nvarchar](max) NULL,
	[enq_type_description] [nvarchar](max) NULL,
	[contact_reason_enq_type] [nvarchar](max) NULL,
	[contact_reason_details1] [nvarchar](max) NULL,
	[contact_reason_details2] [nvarchar](max) NULL,
	[Year] [nvarchar](max) NULL
	
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
