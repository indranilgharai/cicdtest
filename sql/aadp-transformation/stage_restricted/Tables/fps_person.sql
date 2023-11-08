/****** Object:  Table [stage_restricted].[fps_person]    Script Date: 3/31/2022 6:59:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stage_restricted].[fps_person]
(
	[person_uuid] [nvarchar](max) NULL,
	[dob_1] [nvarchar](max) NULL, 
	[first_name_1] [nvarchar](max) NULL, 
	[last_name_1] [nvarchar](max) NULL, 
	[phone_1] [nvarchar](max) NULL, 
	[title_1] [nvarchar](max) NULL, 
	[addresses_L_M_title_1] [nvarchar](max) NULL, 
	[addresses_L_M_phone_1] [nvarchar](max) NULL, 
	[addresses_L_M_line_2_1] [nvarchar](max) NULL, 
	[addresses_L_M_line_1] [nvarchar](max) NULL, 
	[addresses_L_M_last_name_1] [nvarchar](max) NULL, 
	[addresses_L_M_first_name_1] [nvarchar](max) NULL, 
	[historical_phones_1] [nvarchar](max) NULL, 
	[optins_M_email_L_M_reason_1] [nvarchar](max) NULL, 
	[optins_M_email_L_M_last_modified_source] [nvarchar](max) NULL, 
	[optins_M_email_L_M_funnel] [nvarchar](max) NULL, 
	[optins_M_email_L_M_created] [nvarchar](max) NULL, 
	[optins_M_sms_L_M_reason] [nvarchar](max) NULL, 
	[optins_M_sms_L_M_last_modified_source] [nvarchar](max) NULL, 
	[optins_M_sms_L_M_created] [nvarchar](max) NULL, 
	[optins_M_telephonemarketing_L_M_last_modified_source] [nvarchar](max) NULL, 
	[optins_M_telephonemarketing_L_M_created] [nvarchar](max) NULL,
	[optins_M_sms_L_M_id] [nvarchar](max) NULL,
	[optins_M_telephonemarketing_L_M_id] [nvarchar](max) NULL,
	[phone_country_code_1] [nvarchar](max) NULL,
	[addresses_L_M_phone_country_code_1] [nvarchar](max) NULL,
	[tax_exemption_code] [nvarchar](max) NULL,
	[email_1] [nvarchar](max) NULL, 
	[historical_emails_1] [nvarchar](max) NULL,
	[optins_M_email_L_M_id]  [nvarchar](max) NULL,
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
