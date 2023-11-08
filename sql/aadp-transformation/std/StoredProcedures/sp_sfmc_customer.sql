/****** Object:  StoredProcedure [std].[sp_sfmc_customer]    Script Date: 4/12/2022 5:57:38 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create PROC [std].[sp_sfmc_customer] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 

BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN
			
			

truncate table std.sfmc_customer;

WITH senttable AS (
           	SELECT *,
			rank() OVER (PARTITION BY subscriberid ORDER BY [md_record_ingestion_timestamp] desc,md_record_written_timestamp desc, EventDate desc) AS dupcnt
				FROM std.sfmc_sent where triggerersenddefinitionobjectid is not null  
), 
journeyacttable AS (
           	SELECT *,
			rank() OVER (PARTITION BY journeyactivityobjectid ORDER BY [md_record_ingestion_timestamp] desc,md_record_written_timestamp desc) AS dupcnt
			FROM std.sfmc_journeyactivity where versionid is not null
), 
journeytable AS (
           	SELECT *,
			rank() OVER (PARTITION BY versionid ORDER BY [md_record_ingestion_timestamp] desc,md_record_written_timestamp desc) AS dupcnt
			FROM std.sfmc_journey where   versionid is not null
),
svoc AS (
           	SELECT *,
			rank() OVER (PARTITION BY contactkey ORDER BY [md_record_ingestion_timestamp] desc,md_record_written_timestamp desc) AS dupcnt
			FROM std.sfmc_svoc where contactkey is not null
), 
subscribers AS (
           	SELECT *,
			rank() OVER (PARTITION BY subscriberkey ORDER BY [md_record_ingestion_timestamp] desc,md_record_written_timestamp desc) AS dupcnt
			FROM std.sfmc_subscribers where subscriberkey is not null
)



			insert into std.sfmc_customer 
			select distinct  
			cast(trim(contactkey) as varchar(200)) contactkey,
			cast(trim(email) as varchar(200)) svoc_email,
			cast(trim(optinemail) as varchar(200)) optinemail,
			cast(trim(optinmobile) as varchar(200)) optinmobile,
			cast(trim(source) as varchar(200)) svocsource,
			cast(trim(rfv_class) as varchar(200)) rfv_class,
			cast(trim(rfv_segment_name) as varchar(200)) rfv_segment_name,
			cast(trim(journey.journeyname) as varchar(200)) journeyname
			,getDate() as md_record_written_timestamp
			,@pipelineid as md_record_written_pipeline_id
			,@jobid as md_transformation_job_id
			,'DERIVED' as md_source_system			
			from (select * from svoc  where dupcnt=1)svoc
			left join (select * from subscribers  where dupcnt=1) sub
			on svoc.contactkey = sub.subscriberkey
			left join (select * from senttable where dupcnt=1) snt
			on sub.subscriberid = snt.subscriberid 
			left join (select * from journeyacttable  where dupcnt=1) journeyact
			on snt.triggerersenddefinitionobjectid = journeyact.journeyactivityobjectid
			left join (select * from journeytable  where dupcnt=1 and journeyname is not null) journey
			on journeyact.versionid = journey.versionid
			
			OPTION (LABEL = 'AADPSTDSFMCCUST');
			
			UPDATE STATISTICS std.sfmc_customer;

				--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADPSTDSFMCCUST'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label
		END
		ELSE
		
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from std.sfmc_customer;
			SELECT @onlydate=CAST(@newrec as date);
			
			delete from std.sfmc_customer where md_record_written_timestamp=@newrec;
		END

	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.transform_error_log_sp
		  SELECT ERROR_NUMBER() AS ErrorNumber ,
		  ERROR_SEVERITY() AS ErrorSeverity ,
		  ERROR_STATE() AS ErrorState ,
		  'std.sp_sfmc_customer' AS ErrorProcedure ,
		  ERROR_MESSAGE() AS ErrorMessage,
		  getdate() as Updated_date

	END CATCH
		
		
end