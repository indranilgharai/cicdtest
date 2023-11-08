/****** Object:  StoredProcedure [cons_customer].[sp_zendesk_contact_reason]   Script Date: 15/03/2023 04:05:00 PM ******/
/****** Updated SP: Script Date: 19/06/2023 Updated logic after validation and also to include Prior 2021 data ****/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_customer].[sp_zendesk_contact_reason] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
		IF @reset = 0
		BEGIN

IF OBJECT_ID('tempdb..#contactreason') IS NOT  NULL
BEGIN
    DROP TABLE #contactreason
END
create table #contactreason
with
(distribution=round_robin,
clustered index(ticket_id)
)
as 
with custom_fields
    as
    (select u.ticket_id, u.enq_type, u.value
    from
    ( Select ticket_id, cast([channel_labell_enq_type] as nvarchar(500)) as channel_labell_enq_type
        ,cast([checkout_issues] as nvarchar(500)) as checkout_issues,
        cast([click_collect] as nvarchar(500)) as click_collect,
        cast([complaint_theme] as nvarchar(500)) as complaint_theme
        ,cast([customer_service_fb] as nvarchar(500)) as customer_service_fb,
        cast([damaged_prod] as nvarchar(500)) as damaged_prod,
        cast([delivery_info] as nvarchar(500)) as delivery_info,
        cast([delivery_info_req_desc] as nvarchar(500)) as delivery_info_req_desc,
        cast([fb_enq_type] as nvarchar(500)) as fb_enq_type
        ,cast([feedback_theme] as nvarchar(500)) as feedback_theme
        ,cast([general_enq] as nvarchar(500)) as general_enq
        ,cast([gift_card_enq_type] as nvarchar(500)) as gift_card_enq_type
        ,cast([gift_wrapping]  as nvarchar(500)) as gift_wrapping
        ,cast([incorrect_missing_damaged_products_enq] as nvarchar(500)) as incorrect_missing_damaged_products_enq
        ,cast([issues_feedback_desc] as nvarchar(500)) as issues_feedback_desc,
        cast([misspicks] as nvarchar(500)) as misspicks,
        cast([online_order_query_type] as nvarchar(500)) as online_order_query_type
        ,cast([order_amend_enq] as nvarchar(500)) as order_amend_enq
        ,cast([order_enq_type] as nvarchar(500)) as order_enq_type
        ,cast([other_enq_type] as nvarchar(500)) as other_enq_type
        ,cast([payment_checkout_issues] as nvarchar(500)) as payment_checkout_issues
        ,cast([press_marketing_enq] as nvarchar(500)) as press_marketing_enq
        ,cast([privacy_sub_cat] as nvarchar(500)) as privacy_sub_cat
        ,cast([prob_with_prod_enq] as nvarchar(500)) as prob_with_prod_enq
        ,cast([prod]as nvarchar(500)) as prod
        ,cast([prod_adv_recomm_enq] as nvarchar(500)) as prod_adv_recomm_enq
        ,cast([prod_avail_enq] as nvarchar(500)) as prod_avail_enq
        ,cast([prod_enq_type] as nvarchar(500)) as prod_enq_type
        ,cast([prod_query] as nvarchar(500)) as prod_query
        ,cast([prod_range] as nvarchar(500)) as prod_range
        ,cast([prod_usage_guide_enq] as nvarchar(500)) as prod_usage_guide_enq
        ,cast([product_back_in_stock] as nvarchar(500)) as product_back_in_stock
        ,cast([reason_for_cxl_rtn] as nvarchar(500)) as reason_for_cxl_rtn
        ,cast([recall_country] as nvarchar(500)) as recall_country
        ,cast([req_pump_beak_enq] as nvarchar(500)) as req_pump_beak_enq
        ,cast([ret_exch_enq_type] as nvarchar(500)) as ret_exch_enq_type
        ,cast([retail_amenity_business_type] as nvarchar(500)) as retail_amenity_business_type
        ,cast([return] as nvarchar(500)) as [return]
        ,cast([sample_enq] as nvarchar(500)) as sample_enq
        ,cast([sustainability_topics] as nvarchar(500)) as sustainability_topics
        ,cast([track_delivery_info_enq] as nvarchar(500)) as track_delivery_info_enq
        ,cast([understand_more_prod_enq] as nvarchar(500)) as understand_more_prod_enq
        ,cast([user_exp_fb] as nvarchar(500)) as user_exp_fb
        ,cast([web_issues] as nvarchar(500)) as web_issues
        ,cast([website_issues_fb] as nvarchar(500)) as website_issues_fb
        from [std].[zendesk_custom_fields]  ) as my_table
    UNPIVOT
    (
        value
        for enq_type in (
        [channel_labell_enq_type]
        ,[checkout_issues],[click_collect],[complaint_theme]
        ,[customer_service_fb],[damaged_prod],[delivery_info],[delivery_info_req_desc],[fb_enq_type]
        ,[feedback_theme],[general_enq],[gift_card_enq_type],[gift_wrapping],[incorrect_missing_damaged_products_enq]
        ,[issues_feedback_desc],[misspicks],[online_order_query_type],[order_amend_enq],[order_enq_type]
        ,[other_enq_type],[payment_checkout_issues],[press_marketing_enq],[privacy_sub_cat],[prob_with_prod_enq]
        ,[prod],[prod_adv_recomm_enq],[prod_avail_enq],[prod_enq_type],[prod_query],[prod_range],[prod_usage_guide_enq]
        ,[product_back_in_stock],[reason_for_cxl_rtn],[recall_country],[req_pump_beak_enq],[ret_exch_enq_type]
        ,[retail_amenity_business_type],[return],[sample_enq],[sustainability_topics]
        ,[track_delivery_info_enq],[understand_more_prod_enq],[user_exp_fb],[web_issues],[website_issues_fb] 
        )
    ) u 
    ),

      ticket_form_products as (    /* for product and Store & Service feedback */
    
    Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,COALESCE(cus.value, form.contact_reason_enq_type) as contact_reason_enq_type
    ,COALESCE(case when CHARINDEX('__',cus1.value) <> 0 then 
    case when contact_reason in ('Product', 'Store & Service Feedback') then cus1.value 
    ELSE
    SUBSTRING(cus1.value,1,CHARINDEX('__',cus1.value)-1) 
    END
    ELSE cus1.value END, form.contact_reason_details1)  as contact_reason_details1
    ,COALESCE(
        case when CHARINDEX('__',cus2.value) <> 0 then 
    case when contact_reason in ('Product', 'Store & Service Feedback') then cus2.value 
    ELSE 
    
    SUBSTRING(cus2.value,CHARINDEX('__',cus2.value)+2, len(cus2.value)) 
     END
    ELSE cus2.value END,form.contact_reason_details2)  as contact_reason_details2

    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

    left join 

    custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 

    join 

    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type

    left join 

    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where tick.ticket_form_id not in ('360000041095', '360000036696', '360013819095', '360000040276', '360013819535', '360000040316')
    )

	
,ticket_form_orders as ( 

    Select * from ticket_form_products

    UNION

    Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,cus.value as contact_reason_enq_type
    --,COALESCE(cus.value, form.contact_reason_enq_type) as contact_reason_enq_type
    -- ,cus1.value as contact_reason_details1,
    -- cus2.value as contact_reason_details2
    --,form.contact_reason_details1 as f_d
    ,case when CHARINDEX('__',cus1.value) <> 0 then SUBSTRING(cus1.value,1,CHARINDEX('__',cus1.value)-1) 
    ELSE cus1.value END as contact_reason_details1
    ,case when CHARINDEX('__',cus2.value) <> 0 then 
    SUBSTRING(cus2.value,CHARINDEX('__',cus2.value)+2, len(cus2.value)) 
    ELSE 
    cus2.value END as contact_reason_details2
    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

    left join 

    custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 

    left join 

    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type

    left join 

    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where tick.ticket_form_id in ('360000040276') and contact_reason_enq_type <> 'incorrect__missing_or_damaged_products_'
    )

    ,ticket_form_orders_exc as (   /*To exclude certain orders*/
    Select * from ticket_form_orders
    union
    
    Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,cus.value as contact_reason_enq_type
    --,form.contact_reason_details1 as f_d
    ,cus1.value  as contact_reason_details1
    ,cus2.value  as contact_reason_details2
    -- --cus1.value as contact_reason_detail1
    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

    left join 

    custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 

    left join 

    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type

    join 

    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where tick.ticket_form_id in ('360000040276') and contact_reason_enq_type in ('incorrect__missing_or_damaged_products_')
    )

, ticket_form_other as (		/* For Other, Press & Marketing and Gift Card */
Select * from ticket_form_orders_exc
UNION
Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,case when CHARINDEX('__',cus.value) <> 0 then SUBSTRING(cus.value,1,CHARINDEX('__',cus.value)-1) 
    ELSE cus.value END as contact_reason_enq_type
    ,case when CHARINDEX('__',cus.value) <> 0 then 
    SUBSTRING(cus.value,CHARINDEX('__',cus.value)+2, len(cus.value)) 
    ELSE NULL END  as contact_reason_details1
    ,NULL as contact_reason_details2
    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

    left join 

    custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 

    left join 

    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type

    left join 

    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where tick.ticket_form_id in ('360000041095', '360000036696', '360013819535','360013845615')
)

, 

ticket_returns_exchanges as  /* Returns & Exchanges */
(
Select * from ticket_form_other
UNION
Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,cus.value as contact_reason_enq_type
    
    ,cus1.value  as contact_reason_details1

    ,cus1.value as contact_reason_details2
    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

    left join 

    custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type

    left join 

    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type

    where tick.ticket_form_id in ('360013819095')    
)

, union_all_forms_hard_coded as  /*To handle hard coded values */
(
Select * from ticket_returns_exchanges
UNION
Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason

    ,coalesce(cus.value,form.contact_reason_enq_type) as contact_reason_enq_type

    ,case when CHARINDEX('__',cus1.value) <> 0 then SUBSTRING(cus1.value,1,CHARINDEX('__',cus1.value)-1) 
    ELSE cus1.value END  as contact_reason_details1

    ,case when CHARINDEX('__',cus2.value) <> 0 then 
    SUBSTRING(cus2.value,CHARINDEX('__',cus2.value)+2, len(cus2.value)) 
    ELSE NULL END as contact_reason_details2
    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

    left join 

    custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 

    left join 

    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type

    left join 

    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where tick.ticket_form_id in ('360000040316', '360000041075', '4962033298959') or (Year = 'Prior_2021') and tick.ticket_form_id <> '360000041036'
)

, union_all_forms_hard_coded_prior as  /* for the prior 2021 records where hardcoding is required for contact_reason_detail1 */
(
Select * from union_all_forms_hard_coded
UNION
Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,cus.value as contact_reason_enq_type
    ,case when CHARINDEX('__',cus1.value) <> 0 then SUBSTRING(cus1.value,1,CHARINDEX('__',cus1.value)-1) 
    ELSE cus1.value END  as contact_reason_details1
    ,case when CHARINDEX('__',cus2.value) <> 0 then 
    SUBSTRING(cus2.value,CHARINDEX('__',cus2.value)+2, len(cus2.value)) 
    ELSE NULL END as contact_reason_details2
    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

    left join 
	  custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 
	left  join 
	    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type
	left  join 
	    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where tick.ticket_form_id in ('360000041036')
)

, union_all_forms_NULL as  /* for the prior 2021 records where hardcoding is required for contact_reason_detail1 */
(
Select * from union_all_forms_hard_coded_prior
UNION
Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,cus.value as contact_reason_enq_type
    ,cus1.value as contact_reason_details1
    ,cus2.value as contact_reason_details2
    
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

     left join 
	  custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 
	  left join 
	    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type
	  left join 
	    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where cus1.value is NULL and cus2.value is NULL 
    and tick.ticket_form_id not in ('360000041095', '360000036696', '360013819535','360013845615', '360000040316', '360000041075', '4962033298959', '360000040296', '360000021655') 
    and (Year <> 'Prior_2021') 
    and cus.value is NULL
)
, union_all_forms_NULL_fin as  /* for the prior 2021 records where hardcoding is required for contact_reason_detail1 */
(
Select * from union_all_forms_NULL
UNION
Select distinct tick.id as ticket_id,
    form.contact_reason as contact_reason
    ,cus.value as contact_reason_enq_type
    ,cus1.value as contact_reason_details1
    ,NULL as contact_reason_details2
     
    -------------metadata fields----------
    ,getDate() AS md_record_written_timestamp
    ,@pipelineid AS md_record_written_pipeline_id
    ,@jobid AS md_transformation_job_id

    from [std].[zendesk_tickets] tick
    left join (
        select distinct * from 
        [std].[zendesk_contact_reason_mapping] )
        form on tick.ticket_form_id = form.ticket_form_id

     left join 
	  custom_fields cus on tick.id = cus.ticket_id and form.enq_type_description = cus.enq_type 
	  left join 
	    custom_fields cus1 on tick.id = cus1.ticket_id and cus.value = form.contact_reason_enq_type and form.contact_reason_details1= cus1.enq_type
	  left join 
	    custom_fields cus2 on tick.id = cus2.ticket_id and form.contact_reason_details2= cus2.enq_type
    where cus1.value is NULL and cus2.value is NULL 
    and tick.ticket_form_id in ('360000040296', '360000021655') 
    and (Year <> 'Prior_2021') 
)


SELECT
  *
FROM (
    SELECT distinct 
    ticket_id,contact_reason,
    contact_reason_enq_type = MAX(contact_reason_enq_type) OVER (PARTITION BY ticket_id),
      contact_reason_details1 = MAX(contact_reason_details1) OVER (PARTITION BY ticket_id),
      contact_reason_details2= MAX(contact_reason_details2) OVER (PARTITION BY ticket_id),
      md_record_written_timestamp, 
      md_record_written_pipeline_id,
      md_transformation_job_id
    FROM union_all_forms_NULL_fin
    GROUP BY ticket_id, contact_reason,contact_reason_enq_type,
    contact_reason_details1,
    contact_reason_details2,
    md_record_written_timestamp, 
    md_record_written_pipeline_id,
    md_transformation_job_id
) AS dedup;

    MERGE 
	 cons_customer.zendesk_contact_reason as TargetTbl
    USING #contactreason as SourceTbl
    ON  SourceTbl.ticket_id = TargetTbl.ticket_id

    WHEN MATCHED  THEN UPDATE SET
    TargetTbl.[ticket_id]=SourceTbl.[ticket_id],
    TargetTbl.[contact_reason]=SourceTbl.[contact_reason],
    TargetTbl.[contact_reason_enq_type]=SourceTbl.[contact_reason_enq_type],
    TargetTbl.[contact_reason_details1]=SourceTbl.[contact_reason_details1],
    TargetTbl.[contact_reason_details2]=SourceTbl.[contact_reason_details2],
    TargetTbl.[md_record_written_timestamp]=SourceTbl.[md_record_written_timestamp],
    TargetTbl.[md_record_written_pipeline_id]= SourceTbl.[md_record_written_pipeline_id],
    TargetTbl.[md_transformation_job_id]=SourceTbl.[md_transformation_job_id]	

     WHEN NOT MATCHED BY TARGET THEN INSERT 
    ([ticket_id], [contact_reason], [contact_reason_enq_type], [contact_reason_details1], [contact_reason_details2],[md_record_written_timestamp],[md_record_written_pipeline_id], [md_transformation_job_id])
    VALUES 
    (SourceTbl.[ticket_id], SourceTbl.[contact_reason], SourceTbl.[contact_reason_enq_type], SourceTbl.[contact_reason_details1], SourceTbl.[contact_reason_details2], SourceTbl.[md_record_written_timestamp],
     SourceTbl.[md_record_written_pipeline_id], SourceTbl.[md_transformation_job_id]);




	UPDATE STATISTICS [cons_customer].[zendesk_contact_reason];
	
		--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADPCONSSALES'

			EXEC meta_ctl.sp_row_count @jobid ,@step_number ,@label
		END
		ELSE
		BEGIN
			DECLARE @newrec DATETIME ,@onlydate DATE
			SELECT @newrec = max(md_record_written_timestamp) FROM [cons_customer].[zendesk_contact_reason];
			SELECT @onlydate = CAST(@newrec AS DATE);
			
			DELETE FROM [cons_customer].[zendesk_contact_reason] WHERE md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.zendesk_contact_reason' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END