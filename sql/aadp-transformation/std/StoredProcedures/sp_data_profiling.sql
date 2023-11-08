SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_data_profiling] AS

BEGIN
	BEGIN TRY

		--truncate and load data profiling data for latest data written to fps_person and purchase_records
		truncate table std.data_profiling;
		--Table: FPS_PERSON_ALIAS--

		DECLARE @max_date datetime;
		select @max_date=max(CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )) from std.fps_person_alias

		--CUSTOMER_ID--[COMPLETENESS+UNIQUENESS CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'fps' source_system_id
		,'fps_person_alias' table_name
		,'customer_id' column_name
		,null validity
		,1 uniqueness
		,1 completeness
		,concat(COALESCE(p.[customer_id],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person_alias p WITH (NOLOCK)
		JOIN
		(
		SELECT [customer_id]
		,COUNT([customer_id]) total_uuid
		FROM std.fps_person_alias WITH (NOLOCK)
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		GROUP BY [customer_id]
		HAVING COUNT([customer_id]) = 1
		) x ON x.[customer_id]=p.[customer_id]
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		UNION
		SELECT
		'fps' source_system_id
		,'fps_person_alias' table_name
		,'customer_id' column_name
		,null validity
		,0 uniqueness
		,1 completeness
		,concat(COALESCE(p.[customer_id],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person_alias p WITH (NOLOCK)
		JOIN
		(
		SELECT [customer_id]
		,COUNT([customer_id]) total_uuid
		FROM std.fps_person_alias WITH (NOLOCK)
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		GROUP BY [customer_id]
		HAVING COUNT([customer_id]) > 1
		) x ON x.[customer_id]=p.[customer_id]
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		UNION
		SELECT
		'fps' source_system_id
		,'fps_person_alias' table_name
		,'customer_id' column_name
		,null validity
		,0 uniqueness
		,0 completeness
		,concat(COALESCE(p.[customer_id],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person_alias p WITH (NOLOCK)
		where p.customer_id is null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date;

		--EMAIL--[COMPLETENESS+VALIDITY CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'fps' source_system_id
		,'fps_person_alias' table_name
		,'email' column_name
		,0 validity
		,null uniqueness
		,0 completeness
		,concat(COALESCE(p.[customer_id],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person_alias p WITH (NOLOCK)
		where p.[email] IS NULL
		AND CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		UNION
		SELECT
		'fps' source_system_id
		,'fps_person_alias' table_name
		,'email' column_name
		,std.ChkValidEmail([email]) validity
		,null uniqueness
		,1 completeness
		,concat(COALESCE(p.[customer_id],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person_alias p WITH (NOLOCK)
		where p.[email] IS NOT NULL
		AND CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date;

		/*
		--Table: FPS_PERSON--

		DECLARE @max_date datetime;
		select @max_date=max(CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )) from std.fps_person

		--PERSON_UUID--[COMPLETENESS+UNIQUENESS CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'fps' source_system_id
		,'fps_person' table_name
		,'person_uuid' column_name
		,null validity
		,1 uniqueness
		,1 completeness
		,concat(COALESCE(p.[person_uuid],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person p WITH (NOLOCK)
		JOIN
		(
		SELECT [person_uuid]
		,COUNT([person_uuid]) total_uuid
		FROM std.fps_person WITH (NOLOCK)
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		GROUP BY [person_uuid]
		HAVING COUNT([person_uuid]) = 1
		) x ON x.[person_uuid]=p.[person_uuid]
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		UNION
		SELECT
		'fps' source_system_id
		,'fps_person' table_name
		,'person_uuid' column_name
		,null validity
		,0 uniqueness
		,1 completeness
		,concat(COALESCE(p.[person_uuid],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person p WITH (NOLOCK)
		JOIN
		(
		SELECT [person_uuid]
		,COUNT([person_uuid]) total_uuid
		FROM std.fps_person WITH (NOLOCK)
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		GROUP BY [person_uuid]
		HAVING COUNT([person_uuid]) > 1
		) x ON x.[person_uuid]=p.[person_uuid]
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		UNION
		SELECT
		'fps' source_system_id
		,'fps_person' table_name
		,'person_uuid' column_name
		,null validity
		,0 uniqueness
		,0 completeness
		,concat(COALESCE(p.[person_uuid],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person p WITH (NOLOCK)
		where p.person_uuid is null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date;

		--EMAIL--[COMPLETENESS+VALIDITY CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'fps' source_system_id
		,'fps_person' table_name
		,'email' column_name
		,0 validity
		,null uniqueness
		,0 completeness
		,concat(COALESCE(p.[person_uuid],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person p WITH (NOLOCK)
		where p.[email] IS NULL
		AND CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date
		UNION
		SELECT
		'fps' source_system_id
		,'fps_person' table_name
		,'email' column_name
		,std.ChkValidEmail([email]) validity
		,null uniqueness
		,1 completeness
		,concat(COALESCE(p.[person_uuid],'0'),',',COALESCE(p.email,'0')) record_detail
		,getdate() record_timestamp
		FROM std.fps_person p WITH (NOLOCK)
		where p.[email] IS NOT NULL
		AND CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date;
		*/


		--Table: PURCHASE_RECORD 

		--ORDERID--[COMPLETENESS+UNIQUENESS CHECK]--

		DECLARE @max_date_1 datetime;
		select @max_date_1=max(CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )) from std.purchase_record
		DECLARE @datetimeoffset datetimeoffset(3) = '1000-01-01 01:01:01 +10:0';

		INSERT INTO std.data_profiling
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'orderid' column_name
		,null validity
		,1 uniqueness
		,1 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		JOIN
		(
		SELECT [orderid]
		,COUNT([orderid]) total_uuid
		FROM std.purchase_record WITH (NOLOCK)
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		GROUP BY [orderid]
		HAVING COUNT([orderid]) = 1
		) x ON x.[orderid]=p.[orderid]
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		UNION
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'orderid' column_name
		,null validity
		,0 uniqueness
		,1 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		JOIN
		(
		SELECT [orderid]
		,COUNT([orderid]) total_uuid
		FROM std.purchase_record WITH (NOLOCK)
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		GROUP BY [orderid]
		HAVING COUNT([orderid]) > 1
		) x ON x.[orderid]=p.[orderid]
		where CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		UNION
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'orderid' column_name
		,null validity
		,0 uniqueness
		,0 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.orderid is null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1;

		--PRICE--[COMPLETENESS CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'price' column_name
		,null validity
		,null uniqueness
		,0 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.price is null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		UNION
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'price' column_name
		,null validity
		,null uniqueness
		,1 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.price is not null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1;

		--CREATE_DATE_PURCHASE--[COMPLETENESS CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'create_date_purchase' column_name
		,null validity
		,null uniqueness
		,0 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.create_date_purchase is null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		UNION
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'create_date_purchase' column_name
		,null validity
		,null uniqueness
		,1 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.create_date_purchase is not null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1;

		--SHIPPED_DATE--[COMPLETENESS CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'shipped_date' column_name
		,null validity
		,null uniqueness
		,0 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.shipped_date is null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		UNION
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'shipped_date' column_name
		,null validity
		,null uniqueness
		,1 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.shipped_date is not null 
		and CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1;


		--STORX_SBS_NO--[COMPLETENESS+VALIDITY CHECK]--
		INSERT INTO std.data_profiling
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'storx_sbs_no' column_name
		,0 validity
		,null uniqueness
		,0 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.[storx_sbs_no] IS NULL
		AND CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1
		UNION
		SELECT
		'cegid_hybrid_retailPro' source_system_id
		,'purchase_record' table_name
		,'storx_sbs_no' column_name
		,std.ChkValidNumber([storx_sbs_no]) validity
		,null uniqueness
		,1 completeness
		,concat(COALESCE(p.orderid,'0'),',',COALESCE(p.price,'0'),',',COALESCE(p.shipped_date,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset )),',',COALESCE(p.storx_sbs_no,'0'),',',COALESCE(p.create_date_purchase,CAST(convert(datetimeoffset,@datetimeoffset,103) as datetimeoffset ))) record_detail
		,getdate() record_timestamp
		FROM std.purchase_record p WITH (NOLOCK)
		where p.[storx_sbs_no] IS NOT NULL
		AND CAST(convert(datetime,[md_record_written_timestamp],103) as datetime )=@max_date_1;

		--Logging Successful run status
		Insert meta_audit.data_profiling_log_sp
		  SELECT  
		  'std.sp_data_profiling' AS SP_Name , 
		  'Table data_profiling has been loaded successfully' AS Detail,
		  'SUCCESS' AS SP_Status,
		  getdate() as Updated_Date;

		--History table load of new data in table std.data_profiling
		insert into std.data_profiling_hist select source_system_id,table_name,column_name,validity,uniqueness,completeness,record_details,record_timestamp from std.data_profiling;
		
		UPDATE STATISTICS std.data_profiling; 
		UPDATE STATISTICS std.data_profiling_hist; 


	END TRY
	BEGIN CATCH	
	--ERROR OCCURED
	PRINT 'ERROR SECTION INSERT'
	Insert meta_audit.data_profiling_log_sp
		  SELECT  
		  'std.sp_data_profiling' AS SP_Name , 
		  ERROR_MESSAGE() AS Detail,
		  'FAIL' AS SP_Status,
		  getdate() as Updated_Date;

	END CATCH
END