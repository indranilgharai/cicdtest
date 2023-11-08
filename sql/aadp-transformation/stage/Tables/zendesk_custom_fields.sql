/****** Object:  Table [stage].[zendesk_custom_fields]    Script Date: 8/11/2022 10:00:59 AM ******/
/**modified: added aesop_store_counter column date: 23/05/2023**/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [stage].[zendesk_custom_fields]
(
	[ticket_id] [nvarchar](max) NULL,
	[channel_labell_enq_type] [nvarchar](max) NULL,
	[checkout_issues] [nvarchar](max) NULL,
	[click_collect] [nvarchar](max) NULL,
	[comments_in_eng] [nvarchar](max) NULL,
	[complaint_theme] [nvarchar](max) NULL,
	[country_code] [nvarchar](max) NULL,
	[customer_service_fb] [nvarchar](max) NULL,
	[damaged_prod] [nvarchar](max) NULL,
	[delivery_info] [nvarchar](max) NULL,
	[delivery_info_req_desc] [nvarchar](max) NULL,
	[fb_enq_type] [nvarchar](max) NULL,
	[feedback_theme] [nvarchar](max) NULL,
	[general_enq] [nvarchar](max) NULL,
	[gift_card_enq_type] [nvarchar](max) NULL,
	[gift_wrapping] [nvarchar](max) NULL,
	[incorrect_missing_damaged_products_enq] [nvarchar](max) NULL,
	[issues_feedback_desc] [nvarchar](max) NULL,
	[misspicks] [nvarchar](max) NULL,
	[online_order_query_type] [nvarchar](max) NULL,
	[order_amend_enq] [nvarchar](max) NULL,
	[order_enq_type] [nvarchar](max) NULL,
	[other_enq_type] [nvarchar](max) NULL,
	[payment_checkout_issues] [nvarchar](max) NULL,
	[press_marketing_enq] [nvarchar](max) NULL,
	[privacy_sub_cat] [nvarchar](max) NULL,
	[prob_with_prod_enq] [nvarchar](max) NULL,
	[prod] [nvarchar](max) NULL,
	[prod_adv_recomm_enq] [nvarchar](max) NULL,
	[prod_avail_enq] [nvarchar](max) NULL,
	[prod_enq_type] [nvarchar](max) NULL,
	[prod_query] [nvarchar](max) NULL,
	[prod_range] [nvarchar](max) NULL,
	[prod_usage_guide_enq] [nvarchar](max) NULL,
	[product_back_in_stock] [nvarchar](max) NULL,
	[reason_for_cxl_rtn] [nvarchar](max) NULL,
	[recall_country] [nvarchar](max) NULL,
	[req_pump_beak_enq] [nvarchar](max) NULL,
	[ret_exch_enq_type] [nvarchar](max) NULL,
	[retail_amenity_business_type] [nvarchar](max) NULL,
	[return] [nvarchar](max) NULL,
	[sample_enq] [nvarchar](max) NULL,
	[sustainability_topics] [nvarchar](max) NULL,
	[time_spent_last_update_sec] [nvarchar](max) NULL,
	[total_time_spent] [nvarchar](max) NULL,
	[track_delivery_info_enq] [nvarchar](max) NULL,
	[understand_more_prod_enq] [nvarchar](max) NULL,
	[user_exp_fb] [nvarchar](max) NULL,
	[web_issues] [nvarchar](max) NULL,
	[website_issues_fb] [nvarchar](max) NULL,
	[aesop_store_counter] [nvarchar](max) NULL,
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


