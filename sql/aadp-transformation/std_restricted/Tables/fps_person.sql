/****** Object:  Table [std_restricted].[fps_person]    Script Date: 3/31/2022 7:06:10 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std_restricted].[fps_person]
(
	[person_uuid] [varchar](1000) NULL,
	[dob_1] [varchar](1000) NULL,
	[first_name_1] [varchar](1000) NULL,
	[last_name_1] [varchar](1000) NULL,
	[phone_1] [varchar](1000) NULL,
	[title_1] [varchar](1000) NULL,
	[addresses_L_M_title_1] [varchar](1000) NULL,
	[addresses_L_M_phone_1] [varchar](1000) NULL,
	[addresses_L_M_line_2_1] [varchar](1000) NULL,
	[addresses_L_M_line_1] [varchar](1000) NULL,
	[addresses_L_M_last_name_1] [varchar](1000) NULL,
	[addresses_L_M_first_name_1] [varchar](1000) NULL,
	[historical_phones_1] [varchar](1000) NULL,
	[optins_M_email_L_M_reason_1] [varchar](1000) NULL,
	[optins_M_email_L_M_last_modified_source] [varchar](1000) NULL,
	[optins_M_email_L_M_funnel] [varchar](1000) NULL,
	[optins_M_email_L_M_created] [varchar](1000) NULL,
	[optins_M_sms_L_M_reason] [varchar](1000) NULL,
	[optins_M_sms_L_M_last_modified_source] [varchar](1000) NULL,
	[optins_M_sms_L_M_created] [varchar](1000) NULL,
	[optins_M_telephonemarketing_L_M_last_modified_source] [varchar](1000) NULL,
	[optins_M_telephonemarketing_L_M_created] [varchar](1000) NULL,
	[optins_M_sms_L_M_id] [varchar](1000) NULL,
	[optins_M_telephonemarketing_L_M_id] [varchar](1000) NULL,
	[phone_country_code_1] [varchar](1000) NULL,
	[addresses_L_M_phone_country_code_1] [varchar](1000) NULL,
	[tax_exemption_code] [varchar](1000) NULL,
	[email_1] [varchar](1000) NULL,
	[historical_emails_1] [varchar](1000) NULL,
	[optins_M_email_L_M_id] [varchar](1000) NULL,
	[md_record_ingestion_timestamp] [datetime] NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[md_source_system] [varchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [person_uuid] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO