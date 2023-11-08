/* modified SP: derived country_code date:22/05/2023 */
/* modified SP: mapped aesop_store_counter column from source to target */
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_zendesk_custom_fields] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS  

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

insert into [std].[zendesk_custom_fields]
(
    [ticket_id],
	[channel_labell_enq_type] ,
	[checkout_issues] ,
	[click_collect] ,
	[comments_in_eng] ,
	[complaint_theme] ,
	[country_code] ,
	[customer_service_fb] ,
	[damaged_prod] ,
	[delivery_info] ,
	[delivery_info_req_desc] ,
	[fb_enq_type] ,
	[feedback_theme] ,
	[general_enq] ,
	[gift_card_enq_type] ,
	[gift_wrapping] ,
	[incorrect_missing_damaged_products_enq] ,
	[issues_feedback_desc] ,
	[misspicks] ,
	[online_order_query_type] ,
	[order_amend_enq] ,
	[order_enq_type] ,
	[other_enq_type] ,
	[payment_checkout_issues] ,
	[press_marketing_enq] ,
	[privacy_sub_cat] ,
	[prob_with_prod_enq] ,
	[prod] ,
	[prod_adv_recomm_enq] ,
	[prod_avail_enq] ,
	[prod_enq_type] ,
	[prod_query] ,
	[prod_range] ,
	[prod_usage_guide_enq] ,
	[product_back_in_stock] ,
	[reason_for_cxl_rtn] ,
	[recall_country] ,
	[req_pump_beak_enq] ,
	[ret_exch_enq_type] ,
	[retail_amenity_business_type] ,
	[return] ,
	[sample_enq] ,
	[sustainability_topics] ,
	[time_spent_last_update_sec] ,
	[total_time_spent] ,
	[track_delivery_info_enq] ,
	[understand_more_prod_enq] ,
	[user_exp_fb] ,
	[web_issues] ,
	[website_issues_fb] ,
	[aesop_store_counter],
	[md_record_ingestion_timestamp] ,
	[md_record_ingestion_pipeline_id] ,
	[md_source_system] ,
	[md_record_written_timestamp] ,
	[md_record_written_pipeline_id] ,
	[md_transformation_job_id] 
)
SELECT distinct 
    [ticket_id],
	[channel_labell_enq_type] ,
	[checkout_issues] ,
	[click_collect] ,
	[comments_in_eng] ,
	[complaint_theme] ,
	coalesce([country_code],[recall_country]) as [country_code] , /* derived country_code */
	[customer_service_fb] ,
	[damaged_prod] ,
	[delivery_info] ,
	[delivery_info_req_desc] ,
	[fb_enq_type] ,
	[feedback_theme] ,
	[general_enq] ,
	[gift_card_enq_type] ,
	[gift_wrapping] ,
	[incorrect_missing_damaged_products_enq] ,
	[issues_feedback_desc] ,
	[misspicks] ,
	[online_order_query_type] ,
	[order_amend_enq] ,
	[order_enq_type] ,
	[other_enq_type] ,
	[payment_checkout_issues] ,
	[press_marketing_enq] ,
	[privacy_sub_cat] ,
	[prob_with_prod_enq] ,
	[prod] ,
	[prod_adv_recomm_enq] ,
	[prod_avail_enq] ,
	[prod_enq_type] ,
	[prod_query] ,
	[prod_range] ,
	[prod_usage_guide_enq] ,
	[product_back_in_stock] ,
	[reason_for_cxl_rtn] ,
	[recall_country] ,
	[req_pump_beak_enq] ,
	[ret_exch_enq_type] ,
	[retail_amenity_business_type] ,
	[return] ,
	[sample_enq] ,
	[sustainability_topics] ,
	[time_spent_last_update_sec] ,
	[total_time_spent] ,
	[track_delivery_info_enq] ,
	[understand_more_prod_enq] ,
	[user_exp_fb] ,
	[web_issues] ,
	[website_issues_fb] ,
	[aesop_store_counter], /*added new field */
    CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as [md_record_ingestion_timestamp],
    [md_record_ingestion_pipeline_id],
    [md_source_system],
    getdate() as [md_record_written_timestamp],
    @pipelineid [md_record_written_pipeline_id],
    @jobid [md_transformation_job_id]

  FROM [stage].[zendesk_custom_fields]

  	OPTION (LABEL = 'AADPSTDZNDSKTKTS');

    WITH latest_fields AS (
	SELECT
		*,
		rank() OVER (
			PARTITION BY ticket_id
			ORDER BY
                
				[md_record_ingestion_timestamp] DESC,
				[md_record_written_timestamp] DESC
		) AS dupcnt
	FROM
		[std].[zendesk_custom_fields]
)
DELETE FROM
	latest_fields
WHERE
	dupcnt > 1;

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDZNDSKTKTS'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
			
			truncate table [stage].[zendesk_custom_fields]

		END

		

		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.zendesk_custom_fields;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.zendesk_custom_fields where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_zendesk_custom_fields' AS ErrorProcedure , 
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end


  