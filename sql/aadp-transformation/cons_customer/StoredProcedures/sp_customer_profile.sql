/****** Object:  StoredProcedure [cons_customer].[sp_customer_profile]    Script Date: 6/24/2022 5:54:34 AM ******/
/****** Changed SP to remove Journey fields Modified by Harsha Varadhi 9/6/2022 ******/
/****** Changed SP to calculated avg_days_between_transactions at header level Modified by Sonia Lin 20/6/2022 ******/
/****** Changed SP to exclude transactions made in the same day for avg_days_between_transactions field Modified by Sonia Lin 26/6/2022 ******/
/****** Changed SP to include returns so that total_lifetime_value_AUD matches sum of revenue_tax_exc_AUD Modified by Sonia Lin 17/8/2022 ******/
/****** Changed SP to exclude duplicate Click and Collect orders so sum of revenue_tax_exc_AUD in customer profile matches sales detail time Modified by Sonia Lin 26/8/2022 ******/

/****** Changed SP to update return quantity and total price per order line item when there is a Hybris return Modified by Sonia Lin 1/9/2022 ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [cons_customer].[sp_customer_profile] @jobid [int],@step_number [int],@reset [bit],@pipelineid [varchar](500) AS
BEGIN
	BEGIN TRY
		/* Reset value is passed when the Job is triggered
	*/ 
    --Block start


		IF @reset = 0
		BEGIN
         
			--TRUNCATE TABLE cons_customer.customer_profile;
			
            TRUNCATE TABLE cons_customer.customer_profile_temp;
			/*
			Prodcat clause - get the price along with base_sku,product_category etc at line item level
			*/
            IF OBJECT_ID('#purchase_record', 'U') IS NOT NULL             
                        DROP TABLE  #purchase_record;

            
            IF OBJECT_ID('#purchase_record_line_item', 'U') IS NOT NULL             
                        DROP TABLE  #purchase_record_line_item;

            CREATE TABLE #purchase_record
            WITH
            (
                DISTRIBUTION = HASH ( [orderid] ),
                CLUSTERED COLUMNSTORE INDEX
            ) AS SELECT * FROM std.purchase_record with(nolock)

            CREATE TABLE #purchase_record_line_item
            WITH
            (
                DISTRIBUTION = HASH ( [orderid] ),
                CLUSTERED COLUMNSTORE INDEX
            ) AS SELECT * FROM std.purchase_record_line_item with(nolock);

			WITH prodcat (
				customer_id
				,orderid
				,price
				,category
				,product_type_sub_cat
				,base_sku
				,create_date_purchase
				,is_gift_card_order
				,sample_flag
				)
			AS (
				/* Fetch customers orderid with price ,pd details like category and subcategory*/
				SELECT DISTINCT pr.customer_id
					,pr.orderid
					,case when pr.currency_code='AUD' then prli.price else (cast(prli.price as float)/cast(exrate.ex_rate as FLOAT)) END price
					,pro.category
					,product_type_sub_cat
					,base_sku
					,pr.create_date_purchase
					,CASE 
						WHEN UPPER(is_gift_card_order) = 'Y'
							THEN '1'
						WHEN UPPER(is_gift_card_order) = 'YES'
							THEN '1'
						WHEN UPPER(is_gift_card_order) = 'N'
							THEN '0'
						WHEN UPPER(is_gift_card_order) = 'NO'
							THEN '0'
						ELSE is_gift_card_order
						END AS is_gift_card_order
					,sample_flag
				FROM #purchase_record pr
				LEFT JOIN (select * from (select sbs_no,ex_rate,year,month_no,row_number() over(partition by sbs_no order by year desc,month_no desc) rwno from  [std].[exchange_rate_x] )a where rwno=1 ) exrate ON cast(pr.storx_sbs_no AS INT) = cast(exrate.sbs_no AS INT)
				LEFT JOIN (
					SELECT orderid
						,sales_units salesunits
						,revenue_tax_exc_local price
						,product_code
						,sample_flag
					FROM #purchase_record_line_item 
					WHERE (
							UPPER(cancelled_flag) IN ('N')
							OR cancelled_flag IS NULL
							)
						AND (
							UPPER(return_flag) IN ('N')
							OR return_flag IS NULL
							)
					) prli ON pr.orderid = prli.orderid
				LEFT JOIN std.product_x pro ON prli.product_code = pro.description1
					AND pro.category IS NOT NULL
				WHERE category IS NOT NULL
					AND customer_id != ''
				)
				--INSERT INTO cons_customer.customer_profile
			INSERT INTO cons_customer.customer_profile_temp
			SELECT DISTINCT coalesce(fps.customer_id, sfmc.contactkey, purchaserecord.customer_id) AS customer_id
				,sfmc.RFV_Segment_Name AS customer_rfv_segment
				--Active Customer Logic	-first purchase date of customer from purchase_Record
				,CASE 
					WHEN sfmc.optinEmail = 'Active'
						OR sfmc.optinMobile = 'Active'
						THEN 'Y'
					ELSE 'N'
					END AS active_subscriber
				,CASE 
					WHEN cast(ftxndatestore.create_date_purchase AS DATETIME) > cast(dateadd(month, - 3, getdate()) AS DATETIME)
						THEN 'Y'
					ELSE 'N'
					END AS new_customer
				,sub.sbs_region home_region
				,sub.sbs_name home_subsidiary
				,fps.home_store
				,purchaserecord.tottxn AS lifetime_no_of_transactions
				,purchaserecord.totunits AS lifetime_units_sold
				,purchaserecord.avgunits AS average_units_sold_per_transaction
				,purchaserecord.return_quantity AS lifetime_return_quantity
				,cast(purchaserecord.totprice AS FLOAT) AS total_lifetime_value_aud
				,cast(purchaserecord.avgprice AS FLOAT) AS average_transaction_value_aud
				--,sfmc.journeyname AS customer_journey_state /* Removing Journey fields (No current use) as per Emmas discussion */
				,fps.create_date customer_create_date
				,multi_omni.multi_channel
				,CASE 
					WHEN multi_omni.omni_channel > 1
						THEN 'Y'
					ELSE 'N'
					END omni_channel
				,ftxndatestore.create_date_purchase AS first_transaction_date
				,ftxndatestore.store_name AS first_transaction_store
				,stxndatestore.create_date_purchase AS second_transaction_date
				,stxndatestore.store_name AS second_transaction_store
				,ltxndatestore.create_date_purchase AS last_transaction_date
				,ltxndatestore.store_name AS last_transaction_store
				,sfmc.optinEmail email_optin
				,sfmc.optinMobile phone_optin
				,datediff(day, cast(fps.create_date AS DATETIME), getdate()) relationship_tenure_days
				/*Active flag Logic - If the customer's last purchase date lies within last 12 months, then Y */
				,CASE 
					WHEN datediff(month, cast(ltxndatestore.create_date_purchase AS DATETIME), getdate()) <= 12
						THEN 'Y'
					ELSE 'N'
					END AS active_flag
				,pref_channel.channel_id AS preferred_channel
				,pref_store.store_name AS preferred_store
				,ftxndatestoreretail.create_date_purchase AS first_transaction_date_retail
				,ftxndatestoreretail.store_name AS first_transaction_store_retail
				,cast(retdigdeptprice.retail_sum AS FLOAT) AS lifetime_value_aud_retail
				,ftxndatestoredeptstore.create_date_purchase AS first_transaction_date_deptstore
				,ftxndatestoredeptstore.store_name AS first_transaction_store_deptstore
				,cast(retdigdeptprice.ds_sum AS FLOAT) AS lifetime_value_aud_deptstore
				,ftxndatestoredigital.create_date_purchase AS first_transaction_date_digital
				,ftxndatestoredigital.store_name AS first_transaction_store_digital
				,cast(retdigdeptprice.digital_sum AS FLOAT) AS lifetime_value_aud_digital
				,ltxndatestoreretail.create_date_purchase AS last_transaction_date_retail
				,ltxndatestoreretail.store_name AS last_transaction_store_retail
				,ltxndatestoredeptstore.create_date_purchase AS last_transaction_date_deptstore
				,ltxndatestoredeptstore.store_name AS last_transaction_store_deptstore
				,ltxndatestoredigital.create_date_purchase AS last_transaction_date_digital
				,ltxndatestoredigital.store_name AS last_transaction_store_digital
				,skincareprice.priceval AS skincare_revenue_aud
				,bodycareprice.priceval AS bodycare_revenue_aud
				,haircareprice.priceval AS haircare_revenue_aud
				,fragranceprice.priceval AS fragrance_revenue_aud
				,homeprice.priceval AS home_revenue_aud
				,giftprice.priceval AS gift_revenue_aud
				,sampleproduct.sample_to_product AS sample_to_product_days
				,CASE 
					WHEN sampleproduct.sample_to_product IS NULL
						THEN 'N'
					ELSE 'Y'
					END sample_to_product_flag
				,fps.is_aesop_employee AS is_aesop_employee
				,store.channel AS home_store_type
				,getDate() AS md_record_written_timestamp
				,@pipelineid AS md_record_written_pipeline_id
				,@jobid AS md_transformation_job_id
				,datediff(day, cast(ftxndatestore.create_date_purchase AS DATETIME), getdate()) AS days_since_first_transaction
				,datediff(day, cast(ltxndatestore.create_date_purchase AS DATETIME), getdate()) AS days_since_last_transaction
				,avgtxn.avg_days_between_transactions AS avg_days_between_transactions
				,fps.customer_group_id AS customer_discount_group
			FROM std.fps_person_alias fps --all customers
			FULL OUTER JOIN (
				SELECT DISTINCT contactkey
					,RFV_Segment_Name
					,optinEmail
					,optinMobile
					--,journeyname
				FROM std.sfmc_customer
				) sfmc ON cast(trim(sfmc.contactkey) AS VARCHAR) = cast(trim(fps.customer_id) AS VARCHAR)
			FULL OUTER JOIN (
				SELECT customer_id
					,count(orderid) tottxn
					,sum(unit_count) AS totunits
					,avg(unit_count) AS avgunits
					,SUM(return_qty) AS return_quantity
					,sum(price) AS totprice
					,avg(price) AS avgprice
				FROM (
						SELECT DISTINCT a.customer_id
						,a.orderid
						,a.purchase_record_id
						,case when source_system = 'HYBRIS' -- logic added so Hybris return units updated if there is a return
								AND c.return_qty is not null 
							  then (b.salesunits - c.return_qty) 
							  else b.salesunits 
							  END as unit_count
						,case when source_system = 'HYBRIS' -- total price per order line item updated if there is a Hybris return
									and c.return_value is not null 
								then case when a.currency_code <> 'AUD'
										  then ((cast(b.taxexcprice  as float)/cast(exrate.ex_rate as FLOAT)) - (cast(c.return_value as float)/cast(exrate.ex_rate as float))) 
										  else (b.taxexcprice - c.return_value)	
										  end
								else case when a.currency_code='AUD' 
										  then b.taxexcprice 
										  else (cast(b.taxexcprice  as float)/cast(exrate.ex_rate as FLOAT))
										  end
								end price
						,c.return_qty
					FROM #purchase_record a
					LEFT JOIN (select * from (select sbs_no,ex_rate,year,month_no,row_number() over(partition by sbs_no order by year desc,month_no desc) rwno from  [std].[exchange_rate_x] )a where rwno=1 ) exrate ON cast(a.storx_sbs_no AS INT) = cast(exrate.sbs_no AS INT)
					LEFT JOIN (
						SELECT orderid
							,sum(sales_units) salesunits
							--,sum(revenue_tax_inc_aud) price
							,sum(revenue_tax_exc_local) taxexcprice
						FROM #purchase_record_line_item 
						WHERE (
								UPPER(sample_flag) = 'N'
								OR sample_flag IS NULL
								)
							AND (
								UPPER(cancelled_flag) IN ('N')
								OR cancelled_flag IS NULL
								)
								-- logic for excluding returns excluded, returns will be included in total_lifetime_value_aud
						GROUP BY orderid
						) b ON a.orderid = b.orderid
					LEFT JOIN (
						SELECT orderid
							,sum(return_qty) return_qty
							,sum(return_value) return_value
						FROM #purchase_record_line_item 
						WHERE UPPER(return_flag) IN ('Y')
						GROUP BY orderid
						) c ON a.orderid = c.orderid
						-- logic for excluding duplicate Click and Collect orders (Cegid) in revenue_tax_exc_AUD
					where a.orderid NOT IN (SELECT distinct(orderid) from #purchase_record_line_item where product_code = 'CLICKCOLLECT')
				 ) innerpr
				WHERE customer_id IS NOT NULL and customer_id <>''
				GROUP BY customer_id
				) purchaserecord ON cast(trim(purchaserecord.customer_id) AS VARCHAR) = cast(coalesce(fps.customer_id, sfmc.contactkey) AS VARCHAR)
			LEFT JOIN std.store_x store ON trim(cast(fps.home_store AS VARCHAR)) = trim(cast(store.location_code AS VARCHAR))
			LEFT JOIN (
				SELECT customer_id
					--,string_agg(channel_id, '/') multi_channel
					,string_agg(channel_id, '/') within group (order by channel_id) multi_channel
					,count(DISTINCT channel_id) omni_channel
				FROM (
					SELECT DISTINCT customer_id
						,channel_id
					FROM #purchase_record WHERE customer_id IS NOT NULL and customer_id <>''
					) a
				GROUP BY customer_id
				) multi_omni ON cast(trim(multi_omni.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN std.subsidiary_x sub ON trim(cast(store.sbs_no AS VARCHAR)) = trim(cast(sub.sbs_no AS VARCHAR))
			LEFT JOIN (
				--Logic to find the first transaction date and  store for a Customer
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,frowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) ASC
								) frowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record WHERE customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE frowval = 1
					) b
				WHERE orderval = 1
				) ftxndatestore ON cast(trim(ftxndatestore.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic for finding last transaction date and store per customer
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,lrowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) DESC
								) lrowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record WHERE customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE lrowval = 1
					) b
				WHERE orderval = 1
				) ltxndatestore ON cast(trim(ltxndatestore.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find customer's second transaction purchase date and store
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,frowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) ASC
								) frowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record WHERE customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE frowval = 2
					) b
				WHERE orderval = 1
				) stxndatestore ON cast(trim(stxndatestore.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find the customer's  first purchase date in retail channel 
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,frowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) ASC
								) frowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record
							WHERE upper(trim(channel_id)) = 'RETAIL' and  customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE frowval = 1
					) b
				WHERE orderval = 1
				) ftxndatestoreretail ON cast(trim(ftxndatestoreretail.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find customer's  first purchase in  department store channel
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,frowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) ASC
								) frowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record
							WHERE upper(trim(channel_id)) = 'DEPARTMENT STORE' and  customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE frowval = 1
					) b
				WHERE orderval = 1
				) ftxndatestoredeptstore ON cast(trim(ftxndatestoredeptstore.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find customer's  first purchase in  digital store channel
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,frowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) ASC
								) frowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record
							WHERE upper(trim(channel_id)) = 'DIGITAL' and  customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE frowval = 1
					) b
				WHERE orderval = 1
				) ftxndatestoredigital ON cast(trim(ftxndatestoredigital.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN --Logic to find Retail last transaction date from purchase record
				(
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,lrowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) DESC
								) lrowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record
							WHERE upper(trim(channel_id)) = 'RETAIL' and  customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE lrowval = 1
					) b
				WHERE orderval = 1
				) ltxndatestoreretail ON cast(trim(ltxndatestoreretail.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN --Logic to find Dept store last transaction date from purchase record
				(
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,lrowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) DESC
								) lrowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record
							WHERE upper(trim(channel_id)) = 'DEPARTMENT STORE' and  customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE lrowval = 1
					) b
				WHERE orderval = 1
				) ltxndatestoredeptstore ON cast(trim(ltxndatestoredeptstore.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find digital last transaction date from purchase record
				SELECT DISTINCT create_date_purchase
					,store_name
					,customer_id
				FROM (
					SELECT create_date_purchase
						,store_name
						,customer_id
						,lrowval
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_name
							) orderval
					FROM (
						SELECT dense_rank() OVER (
								PARTITION BY customer_id ORDER BY cast(create_date_purchase AS DATETIME) DESC
								) lrowval
							,*
						FROM (
							SELECT DISTINCT customer_id
								,create_date_purchase
								,store_name
							FROM #purchase_record
							WHERE upper(trim(channel_id)) = 'DIGITAL' and  customer_id IS NOT NULL and customer_id <>''
							) pur
						) a
					WHERE lrowval = 1
					) b
				WHERE orderval = 1
				) ltxndatestoredigital ON cast(trim(ltxndatestoredigital.customer_id) AS VARCHAR) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN -- Logic to find preffered Channel 
				(
				SELECT customer_id
					,channel_id
					,channel_count
					,rowval
				FROM (
					SELECT customer_id
						,channel_id
						,channel_count
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY channel_count DESC
								,channel_id
							) rowval
					FROM (
						SELECT customer_id
							,channel_id
							,count(purchase_record_id) channel_count
						FROM #purchase_record
						WHERE channel_id IS NOT NULL
						GROUP BY customer_id
							,channel_id
						) a
					) b
				WHERE rowval = 1
				) pref_channel ON trim(cast(pref_channel.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN -- Logic to find preffered store
				(
				SELECT customer_id
					,store_name
					,store_count
					,rowval
				FROM (
					SELECT customer_id
						,store_name
						,store_count
						,row_number() OVER (
							PARTITION BY customer_id ORDER BY store_count DESC
								,store_name
							) rowval
					FROM (
						SELECT customer_id
							,store_name
							,count(purchase_record_id) store_count
						FROM #purchase_record
						WHERE store_name IS NOT NULL and  customer_id IS NOT NULL and customer_id <>''
						GROUP BY customer_id
							,store_name
						) a
					) b
				WHERE rowval = 1
				) pref_store ON trim(cast(pref_store.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find the Customer Purchase value in Category : SKIN CARE
				SELECT customer_id
					,sum(cast(price AS FLOAT)) AS priceval
				FROM prodcat
				WHERE upper(trim(category)) = 'SKIN CARE'
				GROUP BY customer_id
				) skincareprice ON trim(cast(skincareprice.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find the Customer Purchase value in Category : BODY CARE
				SELECT customer_id
					,sum(cast(price AS FLOAT)) AS priceval
				FROM prodcat
				WHERE upper(trim(category)) = 'BODY CARE'
				GROUP BY customer_id
				) bodycareprice ON trim(cast(bodycareprice.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find the Customer Purchase value in Category : 
				SELECT customer_id
					,sum(cast(price AS FLOAT)) AS priceval
				FROM prodcat
				WHERE upper(trim(category)) = 'FRAGRANCE'
				GROUP BY customer_id
				) fragranceprice ON trim(cast(fragranceprice.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find the Customer Purchase value in Category : Haircare
				SELECT customer_id
					,sum(cast(price AS FLOAT)) AS priceval
				FROM prodcat
				WHERE upper(trim(category)) = 'HAIR CARE'
				GROUP BY customer_id
				) haircareprice ON trim(cast(haircareprice.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find the Customer Purchase value in Category :HOME
				SELECT customer_id
					,sum(cast(price AS FLOAT)) AS priceval
				FROM prodcat
				WHERE upper(trim(category)) = 'HOME'
				GROUP BY customer_id
				) homeprice ON trim(cast(homeprice.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				--Logic to find the Customer Purchase value in Gift Card orders
				SELECT customer_id
					,sum(cast(price AS FLOAT)) AS priceval
				FROM prodcat
				WHERE cast(is_gift_card_order AS INT) = 1
				GROUP BY customer_id
				) giftprice ON trim(cast(giftprice.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN (
				SELECT DISTINCT customer_id
					,sum(price) totalsum
					,sum(CASE 
							WHEN upper(trim(channel_id)) = 'RETAIL'
								THEN price
							ELSE 0
							END) retail_sum
					,sum(CASE 
							WHEN upper(trim(channel_id)) = 'DEPARTMENT STORE'
								THEN price
							ELSE 0
							END) ds_sum
					,sum(CASE 
							WHEN upper(trim(channel_id)) = 'DIGITAL'
								THEN price
							ELSE 0
							END) digital_sum
				FROM (
					SELECT DISTINCT customer_id
						,a.orderid
						,channel_id
						,purchase_record_id
						,unit_count
						,case when a.currency_code='AUD' then b.price else (cast(b.price as float)/cast(exrate.ex_rate as FLOAT)) END price
						FROM (select * from #purchase_record  where  customer_id IS NOT NULL and customer_id <>'') a
					LEFT JOIN (select * from (select sbs_no,ex_rate,year,month_no,row_number() over(partition by sbs_no order by year desc,month_no desc) rwno from  [std].[exchange_rate_x] )a where rwno=1 ) exrate ON cast(a.storx_sbs_no AS INT) = cast(exrate.sbs_no AS INT)
					
					LEFT JOIN (
						SELECT orderid
							,sum(revenue_tax_exc_local) price
						FROM std.purchase_Record_line_item
						WHERE (
								UPPER(sample_flag) = 'N'
								OR sample_flag IS NULL
								)
							AND (
								UPPER(cancelled_flag) IN ('N')
								OR cancelled_flag IS NULL
								)
							AND (
								UPPER(return_flag) IN ('N')
								OR return_flag IS NULL
								)
						GROUP BY orderid
						) b ON trim(a.orderid) = trim(b.orderid)
					) ret
				GROUP BY customer_id
				) retdigdeptprice ON trim(cast(retdigdeptprice.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			LEFT JOIN --Logic to find the days between Sample purchase and full product purchase 
				(
				SELECT customer_id
					,datediff(day, max(cast(sample_purchase_date AS DATETIME)), min(cast(product_purchase_date AS DATETIME))) sample_to_product
					,min(cast(product_purchase_date AS DATETIME)) purch_date
					,max(cast(sample_purchase_date AS DATETIME)) sample_date
				FROM (
					SELECT DISTINCT a.customer_id
						,a.create_date_purchase product_purchase_date
						,b.create_date_purchase sample_purchase_date
					FROM (
						SELECT *
						FROM prodcat
						WHERE UPPER(product_type_sub_cat) NOT IN (
								'SAMPLE'
								,'PREMIUM SAMPLE'
								)
						) a
					LEFT JOIN (
						SELECT *
						FROM prodcat
						WHERE UPPER(product_type_sub_cat) IN (
								'SAMPLE'
								,'PREMIUM SAMPLE'
								)
						) b ON a.customer_id = b.customer_id
						AND a.base_sku = b.base_sku
					WHERE cast(a.create_date_purchase AS DATETIME) >= cast(b.create_date_purchase AS DATETIME)
					) main
				GROUP BY customer_id
				) sampleproduct ON trim(cast(sampleproduct.customer_id AS VARCHAR)) = cast(trim(fps.customer_id) AS VARCHAR)
			LEFT JOIN -- Logic to find the average  days  between transactions of a customer
				(
				SELECT a.customer_id
					,CASE 
						WHEN avg(a.diff) IS NOT NULL
							THEN avg(cast(a.diff AS FLOAT))
						ELSE 0
						END AS avg_days_between_transactions
				FROM (
					SELECT customer_id
						,orderid
						,create_date_purchase thisorderdate
						,lead(create_date_purchase) OVER (
							PARTITION BY customer_id ORDER BY create_date_purchase ASC
							) AS nextorderdate
							-- Added nullif statement to exclude transactions made in same day
						,nullif(datediff(day, create_date_purchase, lead(create_date_purchase) OVER (
								PARTITION BY customer_id ORDER BY create_date_purchase ASC
								)),0) AS diff
					FROM prodcat
					-- Added in Group By cause so avg_days_between_transactions calculated at header level
					GROUP BY customer_id
					,orderid
					,create_date_purchase
					) a
				GROUP BY a.customer_id
				) avgtxn ON trim(cast(avgtxn.customer_id AS VARCHAR)) = cast(trim(purchaserecord.customer_id) AS VARCHAR)
			OPTION (LABEL = 'AADCONSCUSTPRF');

			--BELOW SCRIPT TO GET THE DRIVER TABLE READ COUNT AND TARGET TABLE WRITE COUNT
			DECLARE @label VARCHAR(500)

			SET @label = 'AADCONSCUSTPRF'

			EXEC meta_ctl.sp_row_count @jobid
				,@step_number
				,@label
			--INSERT INTO cons_customer.customer_profile_temp
			truncate table [cons_customer].[customer_profile]

			INSERT INTO [cons_customer].[customer_profile]
			SELECT *
			FROM cons_customer.customer_profile_temp

			UPDATE STATISTICS cons_customer.customer_profile;
			UPDATE STATISTICS cons_customer.customer_profile_temp;
          --Block end
		END
		ELSE
			--##uncomment it after logic for loading column : md_record_written_timestamp is finalised##
		BEGIN
			DECLARE @newrec DATETIME
				,@onlydate DATE

			SELECT @newrec = max(md_record_written_timestamp)
			FROM cons_customer.customer_profile;

			SELECT @onlydate = CAST(@newrec AS DATE);

			--PRINT @onlydate
			DELETE
			FROM cons_customer.customer_profile
			WHERE md_record_written_timestamp = @newrec;
		END
	END TRY

	BEGIN CATCH
		--ERROR OCCURED
		PRINT 'ERROR SECTION INSERT'

		INSERT meta_audit.transform_error_log_sp
		SELECT ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,'cons_customer.sp_customer_profile' AS ErrorProcedure
			,-- here the sp name u give should be exact same value u give for this sp in sp_name column of meta table: [meta_ctl].[transformation_job_steps]
			ERROR_MESSAGE() AS ErrorMessage
			,getdate() AS Updated_date
	END CATCH
END
