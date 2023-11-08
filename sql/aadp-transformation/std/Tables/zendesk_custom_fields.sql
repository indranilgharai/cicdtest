SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [std].[zendesk_custom_fields]
(
	[ticket_id] [int] NOT NULL,
	[channel_labell_enq_type] [nvarchar](100) NULL,
	[checkout_issues] [nvarchar](100) NULL,
	[click_collect] [nvarchar](100) NULL,
	[comments_in_eng] [nvarchar](max) NULL,
	[complaint_theme] [nvarchar](100) NULL,
	[country_code] [nvarchar](30) NULL,
	[customer_service_fb] [nvarchar](100) NULL,
	[damaged_prod] [nvarchar](100) NULL,
	[delivery_info] [nvarchar](100) NULL,
	[delivery_info_req_desc] [nvarchar](500) NULL,
	[fb_enq_type] [nvarchar](100) NULL,
	[feedback_theme] [nvarchar](500) NULL,
	[general_enq] [nvarchar](100) NULL,
	[gift_card_enq_type] [nvarchar](100) NULL,
	[gift_wrapping] [nvarchar](100) NULL,
	[incorrect_missing_damaged_products_enq] [nvarchar](100) NULL,
	[issues_feedback_desc] [nvarchar](100) NULL,
	[misspicks] [nvarchar](100) NULL,
	[online_order_query_type] [nvarchar](100) NULL,
	[order_amend_enq] [nvarchar](100) NULL,
	[order_enq_type] [nvarchar](100) NULL,
	[other_enq_type] [nvarchar](500) NULL,
	[payment_checkout_issues] [nvarchar](100) NULL,
	[press_marketing_enq] [nvarchar](100) NULL,
	[privacy_sub_cat] [nvarchar](100) NULL,
	[prob_with_prod_enq] [nvarchar](100) NULL,
	[prod] [nvarchar](1000) NULL,
	[prod_adv_recomm_enq] [nvarchar](100) NULL,
	[prod_avail_enq] [nvarchar](100) NULL,
	[prod_enq_type] [nvarchar](100) NULL,
	[prod_query] [nvarchar](100) NULL,
	[prod_range] [nvarchar](100) NULL,
	[prod_usage_guide_enq] [nvarchar](100) NULL,
	[product_back_in_stock] [nvarchar](100) NULL,
	[reason_for_cxl_rtn] [nvarchar](100) NULL,
	[recall_country] [nvarchar](100) NULL,
	[req_pump_beak_enq] [nvarchar](100) NULL,
	[ret_exch_enq_type] [nvarchar](100) NULL,
	[retail_amenity_business_type] [nvarchar](100) NULL,
	[return] [nvarchar](30) NULL,
	[sample_enq] [nvarchar](100) NULL,
	[sustainability_topics] [nvarchar](100) NULL,
	[time_spent_last_update_sec] [int] NULL,
	[total_time_spent] [int] NULL,
	[track_delivery_info_enq] [nvarchar](100) NULL,
	[understand_more_prod_enq] [nvarchar](100) NULL,
	[user_exp_fb] [nvarchar](100) NULL,
	[web_issues] [nvarchar](100) NULL,
	[website_issues_fb] [nvarchar](100) NULL,
	[aesop_store_counter] [nvarchar](500) NULL,
	[md_record_ingestion_timestamp] [datetime] NOT NULL,
	[md_record_ingestion_pipeline_id] [nvarchar](500) NULL,
	[md_source_system] [nvarchar](30) NULL,
	[md_record_written_timestamp] [datetime] NOT NULL,
	[md_record_written_pipeline_id] [nvarchar](500) NULL,
	[md_transformation_job_id] [nvarchar](500) NULL
)
WITH
(
	DISTRIBUTION = HASH(ticket_id),
	HEAP
)
GO
