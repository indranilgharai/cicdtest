-- ## SP for load of Standardised table : FM_PRODUCT_FORECAST ##
-- Modified Script [16/08/2022]: changed the partition level while updating the forecast flags, added new forecast flags that identifies latest records until 1, 2 and 3 months ago from today
-- Modified Script [24/08/2022]: changed the filter in the partition, added new forecast flags that identifies latest records until 1, 3 and 6 months ago from today
-- Modified Script [14/09/2022]: changed the update condition in effective_from_dt and removed coelesce to avoid piciking incorrect measures to compare
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_fm_product_forecast] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS

BEGIN
	BEGIN TRY
	--Fetching the latest snapshot from fm table ------
		DECLARE @max_ingestion_date [varchar](500)
		select @max_ingestion_date=max(CAST(convert(datetime,[md_record_ingestion_timestamp],103) as datetime )) from stage.fm_dp;
		
		DECLARE @load_strt_time [datetime]
		SET @load_strt_time=getdate();
		
---temporary table to fetch the lateste snapshot data ---	

		IF OBJECT_ID('tempdb..#product_forecast_temp') IS NOT NULL
		BEGIN
			DROP TABLE #product_forecast_temp
		END
		IF OBJECT_ID('tempdb..#product_forecast_temp1') IS NOT NULL
		BEGIN
			DROP TABLE #product_forecast_temp1
		END
		-----------------------temporary table to load the current snapshot data ---------------------------

		create table #product_forecast_temp
		WITH
		(
		 DISTRIBUTION = ROUND_ROBIN
		 ,HEAP
		)
		AS
		select dp.SKUCODE as sku_code
			,CAST(CONVERT(DATE, dp.md_record_ingestion_timestamp, 103) AS DATE) as forecast_date
			,CAST(CONVERT(DATE, dp.START_DATE_MONTH, 103) AS DATE) as start_date
			,'FALSE' as latest_forecast_flag
			,dp.LOCATIONCODE as location_code
			,dp.CHANNELCODE as channel_code
			,CAST(dp.PROJECTED_DEMAND_UNITS as int) as project_demand_units
			,CAST(dp.CLEARED_ACTUALS_UNITS as int) as cleared_actual_units
			--- changing eff_from_dt as same as forecast date
			,CAST(CONVERT(DATE, dp.md_record_ingestion_timestamp, 103) AS DATE) as eff_from_dt 
			,CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE) as eff_to_dt
			,getdate() as [md_record_written_timestamp]
			,@pipelineid as [md_record_written_pipeline_id]
			,@jobid as [md_transformation_job_id]  
			,'Futuremaster' as md_source_system
			-- added month-1,month-2,month-3 forecast flags
			,'FALSE' as m_1_forecast_flag
			,'FALSE' as m_3_forecast_flag
			,'FALSE' as m_6_forecast_flag
			from stage.fm_dp dp where CAST(convert(datetime,[md_record_ingestion_timestamp],103) as datetime )=@max_ingestion_date 
			and ([PROJECTED_DEMAND_UNITS] is not null
			OR [CLEARED_ACTUALS_UNITS] is not null) 
			and CHANNELCODE is not null;
			
		DECLARE @read_count [int]
		select @read_count=count(*) from #product_forecast_temp;
		
		IF  @read_count > 0
		BEGIN
			
			
			create table #product_forecast_temp1
			WITH
			(
			 DISTRIBUTION = ROUND_ROBIN
			 ,HEAP
			)
			AS
			select 
			distinct
				pf.sku_code
				,pf.forecast_date
				,pf.start_date
				,pf.latest_forecast_flag
				,pf.location_code
				,pf.channel_code
				--check for historic forecast, if exists insert for reporting
				 ,CASE WHEN pf.project_demand_units is null THEN a.project_demand_units ELSE pf.project_demand_units END as project_demand_units
				-- Where cleared actual units is null check for latest sku_code, location_code, forecast_date, channel and return cleared actual units
				 ,CASE WHEN pf.cleared_actual_units is null THEN b.cleared_actual_units ELSE pf.cleared_actual_units END as cleared_actual_units	
				,pf.eff_from_dt 
				,pf.eff_to_dt
				,pf.md_record_written_timestamp
				,pf.md_record_written_pipeline_id
				,pf.md_transformation_job_id  
				,pf.md_source_system
				-- added month-1,month-2,month-3 forecast flags
				,pf.m_1_forecast_flag
				,pf.m_3_forecast_flag
				,pf.m_6_forecast_flag
				FROM #product_forecast_temp pf 
				LEFT JOIN
				--table to identify most recent forecast
				--including start date in the join level
				(SELECT a1.sku_code,a1.location_code,a1.channel_code,a1.start_date,a1.forecast_date,
				ROW_NUMBER() OVER (PARTITION BY a1.SKU_CODE, a1.location_code,a1.channel_code ORDER BY a1.forecast_date DESC) AS GroupRank1, a1.project_demand_units, a1.cleared_actual_units from [std].[fm_product_forecast] a1 
				WHERE a1.project_demand_units is not null and a1.forecast_date=
				(select max(b1.forecast_date) from [std].[fm_product_forecast] b1 where b1.project_demand_units is not null and a1.sku_code=b1.sku_code and a1.location_code=b1.location_code and a1.channel_code=b1.channel_code and a1.start_date=b1.start_date) ) a
				ON pf.sku_code = a.sku_code and pf.location_code = a.location_code and pf.start_date = a.start_date and pf.channel_code = a.channel_code
				LEFT JOIN
				--table linking cleared actuals with a historic forecast for accuracy and bias analysis
				--including start date in the join level
				(SELECT a2.sku_code,a2.location_code,a2.channel_code,a2.start_date,a2.forecast_date,
				ROW_NUMBER() OVER (PARTITION BY a2.SKU_CODE, a2.location_code,a2.channel_code ORDER BY a2.forecast_date DESC) AS GroupRank1, a2.project_demand_units, a2.cleared_actual_units from [std].[fm_product_forecast] a2 
				WHERE a2.cleared_actual_units is not null and a2.forecast_date=
				(select max(b2.forecast_date) from [std].[fm_product_forecast] b2 where b2.cleared_actual_units is not null and a2.sku_code=b2.sku_code and a2.location_code=b2.location_code and a2.channel_code=b2.channel_code and a2.start_date=b2.start_date)) b
				ON pf.sku_code = b.sku_code and pf.location_code = b.location_code and pf.start_date = b.start_date and pf.channel_code = b.channel_code;

			
			
			---UPDATE product_forecast  with eff_to_dt as current_date for existing matched keys---
			DECLARE @update_rec [int]
			select @update_rec=count(*) from #product_forecast_temp1 temp INNER JOIN [std].[fm_product_forecast] tgt 
			ON (tgt.sku_code = temp.sku_code AND tgt.location_code = temp.location_code AND tgt.START_DATE = temp.START_DATE AND tgt.CHANNEL_CODE = temp.CHANNEL_CODE) 
			--removing coalesce
			where ((isnull(tgt.project_demand_units,0)<>isnull(temp.project_demand_units,0)) OR (isnull(tgt.cleared_actual_units,0) <> isnull(temp.cleared_actual_units,0)))
			and tgt.eff_to_dt=CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE);
			PRINT @update_rec
			if @update_rec>0
			BEGIN
				 UPDATE [std].[fm_product_forecast]
				 --- changing eff_to_dt to incoming latest record's effective from date
				 SET eff_to_dt = temp.[eff_from_dt]
				 from #product_forecast_temp1 temp INNER JOIN [std].[fm_product_forecast] tgt 
				ON (tgt.sku_code = temp.sku_code AND tgt.location_code = temp.location_code AND tgt.START_DATE = temp.START_DATE AND tgt.CHANNEL_CODE = temp.CHANNEL_CODE) 
				--removing coalesce
				where ((isnull(tgt.project_demand_units,0)<>isnull(temp.project_demand_units,0)) OR (isnull(tgt.cleared_actual_units,0) <> isnull(temp.cleared_actual_units,0)))
				and tgt.eff_to_dt=CAST(CONVERT(DATE, '31-12-9999 00:00:00', 103) AS DATE);
			
				---insert new records into product_forecast for existing matched keys with a change in any column value---
				insert into [std].[fm_product_forecast]
				select temp.sku_code
					,temp.forecast_date
					,temp.start_date
					,temp.latest_forecast_flag
					,temp.location_code
					,temp.channel_code
					,temp.project_demand_units
					,temp.cleared_actual_units
					,temp.eff_from_dt
					,temp.eff_to_dt
					,temp.md_record_written_timestamp
					,temp.md_record_written_pipeline_id
					,temp.md_transformation_job_id
					,temp.md_source_system
					-- added month-1,month-2,month-3 forecast flags
					,temp.m_1_forecast_flag
					,temp.m_3_forecast_flag
					,temp.m_6_forecast_flag
				from #product_forecast_temp1 temp
				 INNER JOIN [std].[fm_product_forecast] tgt ON (tgt.sku_code = temp.sku_code AND tgt.location_code = temp.location_code AND tgt.START_DATE = temp.START_DATE AND tgt.CHANNEL_CODE = temp.CHANNEL_CODE) 
				 -- removing coalesce
				 where ((isnull(tgt.project_demand_units,0)<>isnull(temp.project_demand_units,0)) OR (isnull(tgt.cleared_actual_units,0) <> isnull(temp.cleared_actual_units,0)))
				 and  tgt.eff_to_dt=temp.eff_from_dt ;
			END
			
	---insert completely new records which have come in new snapshots---
			insert into [std].[fm_product_forecast]
			select src.sku_code
					,src.forecast_date
					,src.start_date
					,src.latest_forecast_flag
					,src.location_code
					,src.channel_code
					,src.project_demand_units
					,src.cleared_actual_units
					,src.eff_from_dt
					,src.eff_to_dt
					,src.md_record_written_timestamp
					,src.md_record_written_pipeline_id
					,src.md_transformation_job_id
					,src.md_source_system 
					-- added month-1,month-2,month-3 forecast flags
					,src.m_1_forecast_flag
					,src.m_3_forecast_flag
					,src.m_6_forecast_flag
			FROM #product_forecast_temp1 src
			LEFT OUTER JOIN [std].[fm_product_forecast] tgt 
			ON (tgt.sku_code = src.sku_code AND tgt.location_code = src.location_code AND tgt.CHANNEL_CODE = src.CHANNEL_CODE AND tgt.START_DATE = src.START_DATE) 
			--changing filter condition fro eff_to_dt
			WHERE tgt.sku_code is null or tgt.location_code is null or tgt.CHANNEL_CODE is null or tgt.START_DATE is null and tgt.eff_to_dt is null;
			
	---delete records (end date the records) from product_forecast which are not present in current snapshot---
	---removing the logic to delete records (end date the records) from product_forecast which are not present in current snapshot
	---updating the forecast flag field---
			update [std].[fm_product_forecast] set latest_forecast_flag = 'FALSE';
			
			WITH lff (sku_code, location_code,channel_code,start_date,forecast_date,GroupRank) AS
				(
				select sku_code, location_code,channel_code,start_date,forecast_date,
				-- changing partition to SKU || Location || Channel || start_date (Forecast Period) level
				DENSE_RANK() OVER (PARTITION BY SKU_CODE, location_code, channel_code, start_date ORDER BY forecast_date DESC) AS GroupRank
				from [std].[fm_product_forecast]
				)
				update u set u.latest_forecast_flag = 'TRUE'
				from [std].[fm_product_forecast] u
					inner join lff s on
						u.sku_code = s.sku_code and
						u.location_code = s.location_code and
						u.channel_code = s.channel_code and
						u.start_date = s.start_date AND
						u.forecast_date=s.forecast_date
					where s.GroupRank =1;
	-- updating month-1,month-2 and month-3 forecast flags
	---updating the month-1 forecast flag field---
			update [std].[fm_product_forecast] set m_1_forecast_flag = 'FALSE';
			
			WITH lff (sku_code, location_code,channel_code,start_date,forecast_date,GroupRank) AS
				(
				select sku_code, location_code,channel_code,start_date,forecast_date,
				-- changing partition to SKU || Location || Channel || start_date (Forecast Period) level
				DENSE_RANK() OVER (PARTITION BY SKU_CODE, location_code, channel_code, start_date ORDER BY forecast_date DESC) AS GroupRank
				--changed the filter to forecast_date
				from [std].[fm_product_forecast] where forecast_date <=  DATEADD(month, -1, getdate()) 
				)
				update u set u.m_1_forecast_flag = 'TRUE'
				from [std].[fm_product_forecast] u
					inner join lff s on
						u.sku_code = s.sku_code and
						u.location_code = s.location_code and
						u.channel_code = s.channel_code and
						u.start_date = s.start_date AND
						u.forecast_date=s.forecast_date
					where s.GroupRank =1;

	---updating the month-3 forecast flag field---
			update [std].[fm_product_forecast] set m_3_forecast_flag = 'FALSE';
			
			WITH lff (sku_code, location_code,channel_code,start_date,forecast_date,GroupRank) AS
				(
				select sku_code, location_code,channel_code,start_date,forecast_date,
				-- changing partition to SKU || Location || Channel || start_date (Forecast Period) level
				DENSE_RANK() OVER (PARTITION BY SKU_CODE, location_code, channel_code, start_date ORDER BY forecast_date DESC) AS GroupRank
				--changed the filter to forecast_date and calculating till month -3 
				from [std].[fm_product_forecast] where forecast_date <=  DATEADD(month, -3, getdate()) 
				)
				update u set u.m_3_forecast_flag = 'TRUE'
				from [std].[fm_product_forecast] u
					inner join lff s on
						u.sku_code = s.sku_code and
						u.location_code = s.location_code and
						u.channel_code = s.channel_code and
						u.start_date = s.start_date AND
						u.forecast_date=s.forecast_date
					where s.GroupRank =1;
		
	---updating the month-6 forecast flag field---
			update [std].[fm_product_forecast] set m_6_forecast_flag = 'FALSE';
			
			WITH lff (sku_code, location_code,channel_code,start_date,forecast_date,GroupRank) AS
				(
				select sku_code, location_code,channel_code,start_date,forecast_date,
				-- changing partition to SKU || Location || Channel || start_date (Forecast Period) level
				DENSE_RANK() OVER (PARTITION BY SKU_CODE, location_code, channel_code, start_date ORDER BY forecast_date DESC) AS GroupRank
				--changed the filter to forecast_date and calculating till month-6
				from [std].[fm_product_forecast] where forecast_date <=  DATEADD(month, -6, getdate()) 
				)
				update u set u.m_6_forecast_flag = 'TRUE'
				from [std].[fm_product_forecast] u
					inner join lff s on
						u.sku_code = s.sku_code and
						u.location_code = s.location_code and
						u.channel_code = s.channel_code and
						u.start_date = s.start_date AND
						u.forecast_date=s.forecast_date
					where s.GroupRank =1;

					UPDATE STATISTICS [std].[fm_product_forecast];
					UPDATE STATISTICS stage.fm_dp; 
		
		END
		DECLARE @write_count [int]
		select @write_count=count(*) from std.fm_product_forecast where md_record_written_timestamp>=@load_strt_time;
		
		insert [meta_ctl].[transform_count_record_table] select (select @jobid),(select @step_number),(select @read_count),(select @write_count),(select getdate());
		
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR IN INSERT section for load of Standardised table:[std].[fm_product_forecast]'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'std.sp_fm_product_forecast' AS ErrorProcedure
			,ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END