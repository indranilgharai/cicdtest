/****** Object:  StoredProcedure [std].[sp_payment_transaction]    Script Date: 4/5/2022 3:00:31 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_payment_transaction] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

			With cegid_order_payments as (
			select *, rank() OVER (PARTITION BY cegid_order_id ORDER BY [md_record_ingestion_timestamp] desc,method desc,correlation_id desc) AS dupcnt
			from [stage].[cegid_order_payments]
			),
			hybris_order_payments as (
			select *, rank() OVER (PARTITION BY body_hybrisorderid ORDER BY [md_record_ingestion_timestamp] desc,header_method desc,header_correlationid desc) AS dupcnt
			from [stage].[hybris_order_payments]
			)

			INSERT INTO [std].[payment_transaction]

			select distinct 
			'C'+cegid_order_id as order_id,
			null as transaction_type,
			paymentType as payment_method,
			amount as amount,
			currency as currency,
			CASE WHEN [time] is NULL THEN NULL
				WHEN [time]='' THEN NULL
				WHEN upper([time]) like '%Z' THEN convert(datetimeoffset,[time])
				ELSE convert(datetimeoffset,concat(substring([time],1,19),substring(replace([time],':',''),18,3),
					':',substring(replace([time],':',''),21,2)) ) END  as payment_date,
			paymentTransactionId as transaction_id,
			null as reference_id,
			null as payment_provider,
			getDate() as md_record_written_timestamp,
			@pipelineid	as md_record_written_pipeline_id,
			@jobid as md_transformation_job_id,
			md_source_system as md_source_system
			from (select distinct * from [cegid_order_payments] where dupcnt=1) c
			
			union 
			
			select distinct 
			'H'+body_hybrisOrderId as order_id,
			transaction_type as transaction_type,
			payment_type as payment_method,
			amount as amount,
			currency as currency,
			CASE WHEN payment_date is NULL THEN NULL
				WHEN payment_date='' THEN NULL
				WHEN upper(payment_date) like '%Z' THEN convert(datetimeoffset,payment_date)
				ELSE convert(datetimeoffset,concat(substring(payment_date,1,19),substring(replace(payment_date,':',''),18,3),
					':',substring(replace(payment_date,':',''),21,2)) ) END  as payment_date,
			transaction_id as transaction_id,
			reference_id as reference_id,
			payment_provider as payment_provider,
			getDate() as md_record_written_timestamp,
			@pipelineid	as md_record_written_pipeline_id,
			@jobid as md_transformation_job_id,
			md_source_system as md_source_system
			from (select distinct * from [hybris_order_payments] where dupcnt=1) h;

			TRUNCATE TABLE [std].[payment_transaction_del];

			INSERT INTO [std].[payment_transaction_del] 
			SELECT DISTINCT 
			[order_id],
			[transaction_type],
			[payment_method],
			[amount],
			[currency],
			[payment_date],
			[transaction_id],
			[reference_id],
			[payment_provider],
			[md_record_written_timestamp],
			[md_record_written_pipeline_id],
			[md_transformation_job_id],
			[md_source_system]				
			from (
			SELECT *,
			rank() OVER (PARTITION BY [order_id],[transaction_type],[payment_method],[amount],[currency],[payment_date],[transaction_id],[reference_id],[payment_provider] ORDER BY md_record_written_timestamp desc) AS dupcnt
			FROM [std].[payment_transaction] )a WHERE dupcnt=1;

			TRUNCATE TABLE [std].[payment_transaction];

			INSERT INTO [std].[payment_transaction] 
			SELECT DISTINCT 
			[order_id],
			[transaction_type],
			[payment_method],
			[amount],
			[currency],
			[payment_date],
			[transaction_id],
			[reference_id],
			[payment_provider],
			[md_record_written_timestamp],
			[md_record_written_pipeline_id],
			[md_transformation_job_id],
			[md_source_system]				
			from [std].[payment_transaction_del] 
			OPTION (LABEL = 'AADSTDPAYMNT');
				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADSTDPAYMNT'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label

			TRUNCATE TABLE [stage].[cegid_order_payments];
			TRUNCATE TABLE [stage].[hybris_order_payments];
			UPDATE STATISTICS [std].[payment_transaction]; 
			
			UPDATE STATISTICS [std].[payment_transaction_del];
			UPDATE STATISTICS [stage].[cegid_order_payments];
			UPDATE STATISTICS [stage].[hybris_order_payments];
		END
	ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.payment_transaction;			
			delete from std.payment_transaction where md_record_written_timestamp=@newrec;
		END
	END TRY

	BEGIN CATCH
	    Insert meta_audit.transform_error_log_sp
	    SELECT ERROR_NUMBER() AS ErrorNumber ,
		ERROR_SEVERITY() AS ErrorSeverity ,
		ERROR_STATE() AS ErrorState ,
		'std.sp_payment_transaction' AS ErrorProcedure ,
		ERROR_MESSAGE() AS ErrorMessage,
		getdate() as Updated_date
	END CATCH

END