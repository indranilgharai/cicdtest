SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [stage].[sp_line_item_union_sources] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
				With hybris_returned_order as (
				select body_hybrisOrderId,entry_number,quantity_returned,returned_date,shipping_returned,md_record_ingestion_timestamp,
				rank() OVER (PARTITION BY body_hybrisOrderId,entry_number ORDER BY [md_record_ingestion_timestamp] desc,returned_date desc) AS dupcnt
				from [stage].[hybris_returned_order_item]
				),
				hybris_cancelled_order as(
				select  distinct body_hybrisOrderId,entry_number,quantity_cancelled,cancelled_date,shipping_returned,md_record_ingestion_timestamp,
				rank() OVER (PARTITION BY body_hybrisOrderId,entry_number ORDER BY [md_record_ingestion_timestamp] desc,cancelled_date desc) AS dupcnt 				
				from [stage].[hybris_cancelled_order_item]
				),
				hybris_order_details as(
				select  distinct *,
				rank() OVER (PARTITION BY body_hybrisOrderId,entry_number ORDER BY [md_record_ingestion_timestamp] desc,header_method desc,header_correlationid desc) AS dupcnt 				
				from [stage].[hybris_order_details]
				)
				
				Insert into stage.line_item_union_sources
				Select distinct
					cast(ISNULL('C'+cegid_order_id,'') as varchar)+cast(entry_number as varchar) AS Line_item_id
					,'C'+cegid_order_id AS order_id
					,entry_number AS entry_number
					,quantity AS quantity
					,currency_iso AS total_price_currency_iso
					,NULL AS total_price_price_type
					,NULL AS orig_total_price_value
					,cast((quantity*total_price_no_tax) as float) AS total_price_value
					,product_code AS product_code
					,product_name AS product_name
					,NULL AS product_url
					,NULL AS product_purchasable
					,NULL AS product_variant_type
					,NULL AS tax_id
					,cast(total_tax as float) AS tax_rate
					,NULL AS tax_rate_gsthst
					,NULL AS tax_rate_pst
					,upper(md_source_system) as source_system
					,CAST(CONVERT(DATETIME, md_record_ingestion_timestamp, 103) AS DATETIME) as ingestion_timestamp
					,null as sbs_no
					,null as create_date_purchase
					,CASE WHEN quantity<0 THEN 'Y' ELSE 'N' END as return_flag
					,CASE WHEN quantity<0 THEN abs(quantity) ELSE 0 END as return_qty
					,CASE WHEN quantity<0 THEN cast((abs(quantity)*total_price_no_tax) as float) ELSE 0.000 END as return_value
					,null as return_shipping_flag
					,null as return_shipping_value
					,null as return_date
					,null as cancelled_flag
					,null as cancellation_qty
					,null as cancellation_value
					,null as cancellation_shipping_flag
					,null as cancellation_shipping_value
					,null as cancellation_date
					,getDate() as md_record_written_timestamp
					,@pipelineid as md_record_written_pipeline_id
					,@jobid as md_transformation_job_id
					,md_source_system as md_source_system
					,null as promotion_code
	
					FROM [stage].[cegid_order_detail]
					WHERE cegid_order_id IS NOT NULL

				UNION 
				
				SELECT DISTINCT    
					'H'+hod.body_hybrisOrderId+cast(hod.entry_number as varchar)  AS Line_item_id
					,'H'+hod.body_hybrisOrderId AS order_id
					,hod.entry_number AS entry_number
					,quantity AS quantity
					,total_price_currency_iso AS total_price_currency_iso
					,total_price_price_type AS total_price_price_type
					,cast(orig_total_price_value as float) AS orig_total_price_value
					,cast(orig_total_price_value as float) AS total_price_value
					,product_code AS product_code
					,product_name AS product_name
					,product_url AS product_url
					,CASE WHEN product_purchasable=1 THEN 'Y' ELSE 'N' END AS product_purchasable
					,product_variant_type AS product_variant_type
					,tax_id AS tax_id
					,cast(tax_rate as float) AS tax_rate
					,cast(tax_rate_gsthst as float) AS tax_rate_gsthst
					,cast(tax_rate_pst as float) AS tax_rate_pst
					,upper(md_source_system) AS source_system
					,CAST(CONVERT(DATETIME, hod.md_record_ingestion_timestamp, 103) AS DATETIME)	as ingestion_timestamp
					,null as sbs_no
					,created as create_date_purchase
					,CASE WHEN quantity_returned>0 THEN 'Y' ELSE 'N' END as return_flag
					,isnull(quantity_returned,0) as return_qty
					--Updated quantity code to handle divided by zero error
					,CASE WHEN quantity_returned>0 and quantity>0 THEN cast(((orig_total_price_value/quantity)*roi.quantity_returned) as float) ELSE 0 END as return_value
					,case when roi.shipping_returned=1 then 'Y' else 'N' end as return_shipping_flag
					,null as return_shipping_value
					,returned_date as return_date
					,CASE WHEN quantity_cancelled>0 THEN 'Y' ELSE 'N' END as cancelled_flag
					,isnull(quantity_cancelled,0) as cancellation_qty
					,CASE WHEN quantity_cancelled>0 and quantity>0 THEN cast(((orig_total_price_value/quantity)*coi.quantity_cancelled) as float) ELSE 0 END as cancellation_value
					,case when coi.shipping_returned=1 then 'Y' else 'N' end as cancellation_shipping_flag
					,null as cancellation_shipping_value
					,cancelled_date as cancellation_date
					,getDate() as md_record_written_timestamp
					,@pipelineid as md_record_written_pipeline_id
					,@jobid as md_transformation_job_id
					,md_source_system as md_source_system
					,promotion_code as promotion_code
				FROM (select * from hybris_order_details  where dupcnt=1) hod
				
				LEFT JOIN (select * from hybris_returned_order  where dupcnt=1) roi
				on hod.body_hybrisOrderId=roi.body_hybrisOrderId 
					and hod.entry_number=roi.entry_number
					
				LEFT JOIN (select * from hybris_cancelled_order  where dupcnt=1) coi				
					on hod.body_hybrisOrderId=coi.body_hybrisOrderId 
					and hod.entry_number=coi.entry_number
				WHERE hod.body_hybrisOrderId IS NOT NULL	
				
				OPTION (LABEL = 'AADPSTGLINEITEM');

				UPDATE STATISTICS [stage].[cegid_order_detail];
				UPDATE STATISTICS stage.hybris_order_details;
				UPDATE STATISTICS stage.line_item_union_sources;

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
				DECLARE @label varchar(500)
				SET @label='AADPSTGLINEITEM'
				EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

				TRUNCATE TABLE [stage].[hybris_returned_order_item];
				TRUNCATE TABLE [stage].[hybris_cancelled_order_item];
				TRUNCATE TABLE [stage].[hybris_order_details];
				TRUNCATE TABLE [stage].[cegid_order_detail];
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from stage.line_item_union_sources;
			SELECT @onlydate=CAST(@newrec as date);
			
			DELETE	FROM stage.line_item_union_sources where md_record_written_timestamp=@newrec;
		END
	END TRY
	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'
		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
		,ERROR_SEVERITY() AS ErrorSeverity
		,ERROR_STATE() AS ErrorState
		,'stage.sp_line_item_union_sources' AS ErrorProcedure
		,ERROR_MESSAGE() AS ErrorMessage
		,getdate() AS Updated_date

	END CATCH
END