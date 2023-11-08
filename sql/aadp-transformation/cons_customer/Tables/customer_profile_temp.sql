/****** Object:  Table [cons_customer].[customer_profile_history_dev]    Script Date: 3/22/2022 6:44:58 AM ******/
/****** Object:  Modified Table [cons_customer].[customer_profile_temp]    Modified Date: 3/22/2022 6:44:58 AM ******/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [cons_customer].[customer_profile_temp]
(
	[customer_id] [varchar](100) NULL,
	[customer_rfv_segment] [varchar](250) NULL,
	[active_subscriber] [varchar](10) NOT NULL,
	[new_customer] [varchar](10) NOT NULL,
	[home_region] [varchar](50) NULL,
	[home_subsidiary] [varchar](100) NULL,
	[home_store] [varchar](200) NULL,
	[lifetime_no_of_transactions] [int] NULL,
	[lifetime_units_sold] [int] NULL,
	[average_units_sold] [int] NULL,
	[lifetime_return_quantity] [int] NULL,
	[total_lifetime_value_aud] [float] NULL,
	[average_transaction_value_aud] [float] NULL,
	--[customer_journey_state] [varchar](250) NULL, /* Removed Journey State column */
	[customer_create_date] [date] NULL,
	[multi_channel] [varchar](500) NULL,
	[omni_channel] [varchar](10) NOT NULL,
	[first_transaction_date] [date] NULL,
	[first_transaction_store] [varchar](100) NULL,
	[second_transaction_date] [date] NULL,
	[second_transaction_store] [varchar](100) NULL,
	[last_transaction_date] [date] NULL,
	[last_transaction_store] [varchar](100) NULL,
	[email_optin] [varchar](30) NULL,
	[phone_optin] [varchar](30) NULL,
	[relationship_tenure_days] [int] NULL,
	[active_flag] [varchar](10) NOT NULL,
	[preferred_channel] [varchar](100) NULL,
	[preferred_store] [varchar](100) NULL,
	[first_transaction_date_retail] [date] NULL,
	[first_transaction_store_retail] [varchar](100) NULL,
	[lifetime_value_aud_retail] [float] NULL,
	[first_transaction_date_deptstore] [date] NULL,
	[first_transaction_store_deptstore] [varchar](100) NULL,
	[lifetime_value_aud_deptstore] [float] NULL,
	[first_transaction_date_digital] [date] NULL,
	[first_transaction_store_digital] [varchar](100) NULL,
	[lifetime_value_aud_digital] [float] NULL,
	[last_transaction_date_retail] [date] NULL,
	[last_transaction_store_retail] [varchar](100) NULL,
	[last_transaction_date_deptstore] [date] NULL,
	[last_transaction_store_deptstore] [varchar](100) NULL,
	[last_transaction_date_digital] [date] NULL,
	[last_transaction_store_digital] [varchar](100) NULL,
	[skincare_revenue_aud] [float] NULL,
	[bodycare_revenue_aud] [float] NULL,
	[haircare_revenue_aud] [float] NULL,
	[fragrance_revenue_aud] [float] NULL,
	[home_revenue_aud] [float] NULL,
	[gift_revenue_aud] [float] NULL,
	[sample_to_product_days] [int] NULL,
	[sample_to_product_flag] [varchar](2) NULL,
	[is_aesop_employee] [varchar](10) NULL,
	[home_store_type] [varchar](100) NULL,
	[md_record_written_timestamp] [datetime] NULL,
	[md_record_written_pipeline_id] [varchar](500) NULL,
	[md_transformation_job_id] [varchar](500) NULL,
	[days_since_first_transaction] [int] NULL,
	[days_since_last_transaction] [int] NULL,
	[avg_days_between_transactions] [float] NULL,
	[customer_discount_group] [varchar](200) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO