/****** Object:  StoredProcedure [std].[sp_dimitem_location]    Script Date: 1/24/2023 3:05:03 PM ******/
/****** Object:  Modified StoredProcedure [std].[sp_dimitem_location]   Modified Date: 19/04/2023 1:52:00 PM Modified by : Harsha Varadhi ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [std].[sp_dimitem_location] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS 
BEGIN
	BEGIN TRY
		IF @reset=0
		BEGIN

						
			IF OBJECT_ID('tempdb..#dimitem_location') IS NOT  NULL
				BEGIN
					DROP TABLE #dimitem_location
				END
			
				create table #dimitem_location
						with (distribution=round_robin,
						clustered index(locationid_skucode))
						as 
				WITH netsuite_item AS (
							select
								distinct *,
								rank() OVER (
									PARTITION BY itemid
									order by
										lastmodifieddate desc, cast(md_record_written_timestamp as date) desc
								) as row_num
							from
								[std].[netsuite_item]
						),
						currentstdcost AS (
							select
								*,
								row_number() OVER (
									PARTITION BY item,
									location
									order by
										currentstandardcost desc
								) as row_num
							from
								stage.netsuite_aggregateitemlocation_currentstandardcost
						),
						base as(
							select
								*
							from
								(
									select
										distinct location_code,
										netsuite_location,
										sbs_no
									from
										[std].[netsuite_location_combined]
									where
										location_code is not null
								) loc,
								(
									select
										distinct product_id as sku,
										netsuite_id as item_id
									from
										[std].[netsuite_product_combined]
									where
										product_id is not null
								) prods
						) 

						SELECT sub.Sku_Code
								,sub.locationID
								,sub.locationid_skucode
								,sub.cost
								,sub.costingMethod
								,sub.currentStandardCost
								,sub.averageCost
								,sub.lastpurchaseprice
								,sub.excluded_from_stock
								,sub.eff_from_date
								,sub.eff_to_date
								,sub.active_record
								,null as hash_value
								,sub.md_record_written_timestamp
								,sub.md_record_written_pipeline_id
								,sub.md_transformation_job_id


								
						from
						(select
							distinct main.sku as Sku_Code,
							main.location_code as locationID,
							concat(main.location_code,main.sku) as locationid_skucode,
							CASE
								WHEN costingmethod = 'STANDARD' THEN CASE
									WHEN cost > 0 THEN cost
									WHEN isnull(cost, 0) = 0
									and averagecost > 0 THEN averagecost
									WHEN isnull(cost, 0) = 0
									and isnull(averagecost, 0) = 0
									and lastpurchaseprice > 0 THEN lastpurchaseprice
									WHEN isnull(cost, 0) = 0
									and isnull(averagecost, 0) = 0
									and isnull(lastpurchaseprice, 0) = 0 THEN currentStandardCost
								END
								WHEN costingmethod = 'AVG' THEN CASE
									WHEN averagecost > 0 THEN averagecost
									WHEN isnull(averagecost, 0) = 0
									and lastpurchaseprice > 0 THEN lastpurchaseprice
									WHEN isnull(averagecost, 0) = 0
									and isnull(lastpurchaseprice, 0) = 0 THEN cost
								END
							END as cost,
							costingMethod,
							csc.currentStandardCost as currentStandardCost,
							averagecost as averageCost,
							lastpurchaseprice,
							case
								when sku_list.SKU is not null then 'Y'
								else 'N'
							end as excluded_from_stock,
							createddate as eff_from_date,
							NULL as eff_to_date,
							1 as active_record,
							getDate() AS md_record_written_timestamp,
						    @pipelineid   AS md_record_written_pipeline_id,
						    @jobid  AS md_transformation_job_id
							
						from
							base main
							left join netsuite_item ni on main.sku = ni.itemid and ni.row_num = 1 
							left join currentstdcost csc on main.item_id = csc.item and main.netsuite_location = csc.location and csc.row_num = 1
							left join std.itemcost_excluded_stock_list sku_list on main.sku = sku_list.SKU and main.location_code = sku_list.store_no
							where csc.item is not null and csc.location is not null
							
							) sub
							;


/*updating active flag for older matched records to 0 and inserting new matched rows*/		
MERGE INTO [std].[dimitem_location] as TargetTbl 
USING #dimitem_location as SourceTbl ON SourceTbl.locationid_skucode=TargetTbl.locationid_skucode 
	
	WHEN MATCHED AND TargetTbl.[active_record]=1
				 AND (TargetTbl.[Sku_Code] <> SourceTbl.[Sku_Code]
					  OR TargetTbl.[locationID] <> SourceTbl.[locationID]
					  OR TargetTbl.[locationid_skucode] <> SourceTbl.[locationid_skucode]
					  OR TargetTbl.[cost] <> SourceTbl.[cost]
					  OR TargetTbl.[costingMethod] <> SourceTbl.[costingMethod]
					  OR TargetTbl.[currentStandardCost] <> SourceTbl.[currentStandardCost]
					  OR TargetTbl.[averageCost] <> SourceTbl.[averageCost]
					  OR TargetTbl.[lastpurchaseprice] <> SourceTbl.[lastpurchaseprice]
					  OR TargetTbl.[excluded_from_stock] <> SourceTbl.[excluded_from_stock]
					  OR TargetTbl.[eff_from_date] <> SourceTbl.[eff_from_date]
					  OR TargetTbl.[eff_to_date] <> SourceTbl.[eff_to_date])
				 
	
	THEN UPDATE SET TargetTbl.[active_record]=0,TargetTbl.eff_to_date=SourceTbl.eff_from_date
	
	WHEN NOT MATCHED BY TARGET THEN
	
	INSERT ([Sku_Code],[locationID],[locationid_skucode],[cost],[costingMethod],[currentStandardCost],[averageCost],[lastpurchaseprice],[excluded_from_stock],[eff_from_date],[eff_to_date],[active_record],[hash_value],[md_record_written_timestamp],[md_record_written_pipeline_id],[md_transformation_job_id])
	VALUES (SourceTbl.[Sku_Code],SourceTbl.[locationID],SourceTbl.[locationid_skucode],SourceTbl.[cost],SourceTbl.[costingMethod],SourceTbl.[currentStandardCost],SourceTbl.[averageCost],SourceTbl.[lastpurchaseprice],SourceTbl.[excluded_from_stock],SourceTbl.[eff_from_date],SourceTbl.[eff_to_date],SourceTbl.[active_record],SourceTbl.[hash_value],SourceTbl.[md_record_written_timestamp],SourceTbl.[md_record_written_pipeline_id],SourceTbl.[md_transformation_job_id])
	;
	
/*inserting new not matching records*/	

INSERT INTO [std].[dimitem_location]([Sku_Code],[locationID],[locationid_skucode],[cost],[costingMethod],[currentStandardCost],[averageCost],[lastpurchaseprice],[excluded_from_stock],[eff_from_date],[eff_to_date],[active_record],[hash_value],[md_record_written_timestamp],[md_record_written_pipeline_id],[md_transformation_job_id])
SELECT distinct SourceTbl.[Sku_Code],SourceTbl.[locationID],SourceTbl.[locationid_skucode],SourceTbl.[cost],SourceTbl.[costingMethod],SourceTbl.[currentStandardCost],SourceTbl.[averageCost],SourceTbl.[lastpurchaseprice],SourceTbl.[excluded_from_stock],SourceTbl.[eff_from_date],SourceTbl.[eff_to_date],SourceTbl.[active_record],SourceTbl.[hash_value],SourceTbl.[md_record_written_timestamp],SourceTbl.[md_record_written_pipeline_id],SourceTbl.[md_transformation_job_id]

from #dimitem_location as SourceTbl
inner join (
	Select * from [std].[dimitem_location] where active_record = 1 /* Added active filter */
	) as TargetTbl on SourceTbl.locationid_skucode=TargetTbl.locationid_skucode
where (TargetTbl.[Sku_Code] <> SourceTbl.[Sku_Code]
					  OR TargetTbl.[locationID] <> SourceTbl.[locationID]
					  OR TargetTbl.[locationid_skucode] <> SourceTbl.[locationid_skucode]
					  OR TargetTbl.[cost] <> SourceTbl.[cost]
					  OR TargetTbl.[costingMethod] <> SourceTbl.[costingMethod]
					  OR TargetTbl.[currentStandardCost] <> SourceTbl.[currentStandardCost]
					  OR TargetTbl.[averageCost] <> SourceTbl.[averageCost]
					  OR TargetTbl.[lastpurchaseprice] <> SourceTbl.[lastpurchaseprice]
					  OR TargetTbl.[excluded_from_stock] <> SourceTbl.[excluded_from_stock]
					  OR TargetTbl.[eff_from_date] <> SourceTbl.[eff_from_date]
					  OR TargetTbl.[eff_to_date] <> SourceTbl.[eff_to_date]);
			

--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label varchar(500)
			SET @label='AADCONSINVSKU'
			EXEC meta_ctl.sp_row_count @jobid,@step_number,@label				
		
		END
		ELSE
		BEGIN
			DECLARE @newrec datetime, @onlydate date
			select @newrec=max(md_record_written_timestamp) from [std].[dimitem_location] ;
			
			delete from [std].[dimitem_location] where md_record_written_timestamp=@newrec;
			
		END
END TRY

BEGIN CATCH

    Insert meta_audit.transform_error_log_sp
    SELECT ERROR_NUMBER() AS ErrorNumber ,
	ERROR_SEVERITY() AS ErrorSeverity ,
	ERROR_STATE() AS ErrorState ,
	'std.sp_dimitem_location' AS ErrorProcedure ,
	ERROR_MESSAGE() AS ErrorMessage,
	getdate() as Updated_date


END CATCH

END
