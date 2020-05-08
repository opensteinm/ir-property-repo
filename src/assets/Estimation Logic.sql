

-- parameters are for if we want to use the cv value or not based on days btween sale and cv vlaue and then if we use the cv add what % premium to is to get better score
-- 21 136 for variables values
-- see if verison changes.
-- OF NOTE : possible issue in that the 21% premium calculatioin is auto used of the aggregate with out comparing between the two values. just simply uses the 21% based on sales dates vs CV reviews
/*
Version changes:
-----------------------------------------------------------------
0.5 added the indicator for transmission sales so it can be excluded in queries using data DIPPRP-136 20/04/2020
0.4 added MAX and group by o base zero query for DIPPRP-129 3/4/2020
0.3 Added settlement date DIPPRP-128
0.2 Schema changes for source tables in relation to DIPPRP-107
0.1 Initial draft of code for UAT and Review
-----------------------------------------------------------------

select    
        concat(cast(round(SUM(isnull(accuracy_category_pass_val,0)) / (SUM(isnull(accuracy_category_pass_val,0))+SUM(isnull(accuracy_category_fail_val,0)))    * 100.0 , 1) as string ) , '%') 
        as accuracy_score , 'Score based on estimates falling into 70% and 130% of the actual sales price when available (multi-sales excluded).  $0 sales and Multi-sales will use the generated sales values.' as notes
        from (
select 
    case 
        when accuracy_category =  '[1] Acceptable Inferred Value' then accuracy_category_count  end as accuracy_category_pass_val, 
        
        sum (case when accuracy_category !=  '[1] Acceptable Inferred Value' then accuracy_category_count end) AS accuracy_category_fail_val
        from (

select accuracy_category, count(*) as accuracy_category_count */
-- quality_estimation_level, count(*) as quality_estimation_level_count from (
/*Notes: 

(1) Known issue is that the table from core logic cp_pty_cl_sale doesn't 
	have all the sales records e.g. prior to 2008
*/
--possible issue where agg_marker_cap_val_est and the FR6 data are correct and logic priorities agg_marker_cap_val_est (a Q2 rating value over a Q1 rating value)
--from (
--## base data from here

select estimation_quality.*,

-- need 2 check how close the agg_marker_cap_val_est and fr06_data values are if within 30% of each other then its a higher quality rating than just a agg_marker_cap_val_est without a % 
case    	
			-- no estimation provided for these scenarios of data
			when final_estimation_data = 0   then 'Quality Level -1'  
			when FR06_logic_group = 'FR-06 Scenario Out of Scope' then 'Quality Level -1' 
			
			-- estimation provided based on fr06_data returning a calculated result and not using and CV values 
	        when final_estimation_data = fr06_data and final_estimation_data != fr06_default_cv_value and final_estimation_data != fr07_data_cv_pc_incr then 'Quality Level 1' 
			
			-- the two checks below for QL2 and 3 are to see if the agg marker is not a CV value and is 30% of the fr6 data
			when final_estimation_data = agg_marker_cap_val_est and final_estimation_data  != fr07_data_cv_pc_incr and final_estimation_data != fr06_default_cv_value  and round(agg_marker_cap_val_est * 100.0 / fr06_data  ,1)  >= 70 
			OR 
                 final_estimation_data = agg_marker_cap_val_est and final_estimation_data  != fr07_data_cv_pc_incr and final_estimation_data != fr06_default_cv_value and round(agg_marker_cap_val_est * 100.0 / fr06_data  ,1) <=130 then 'Quality Level 2'  

            when final_estimation_data = agg_marker_cap_val_est and final_estimation_data  != fr07_data_cv_pc_incr and final_estimation_data != fr06_default_cv_value and round(agg_marker_cap_val_est * 100.0 / fr06_data  ,1)  < 70 
			OR
                 final_estimation_data = agg_marker_cap_val_est and final_estimation_data  != fr07_data_cv_pc_incr and final_estimation_data != fr06_default_cv_value and round(agg_marker_cap_val_est * 100.0 / fr06_data  ,1) > 130 then 'Quality Level 3' 

            when final_estimation_data = agg_marker_cap_val_est and  nonnullvalue(fr06_data)=false  and final_estimation_data != fr07_data_cv_pc_incr and final_estimation_data != fr06_default_cv_value then 'Quality Level 3' 
            
			-- checks if the value is the cv with the premium added
			when final_estimation_data = fr07_data_cv_pc_incr and final_estimation_data = agg_marker_cap_val_est then 'Quality Level 4'
			when final_estimation_data = fr07_data_cv_pc_incr and agg_marker_cap_val_est =0 then 'Quality Level 4'
			
			-- checks if the value is the cv with the 200K default added for multi sale sales.
			when final_estimation_data = fr06_default_cv_value and final_estimation_data=fr06_data then 'Quality Level 5' 
			when final_estimation_data = fr06_default_cv_value and fr06_data=0 then 'Quality Level 5' 
			else 'Quality Level -1'
			end as quality_estimation_level
			--round(agg_marker_cap_val_est * 100.0 / fr06_data  ,1) as percentage_estimate_debug -- use dfor debugging
from (

--select FR06_Logic_Group, COUNT(*) from (
--add logic to prioritize the CV if the SALE of the property is within 1 months of the CV
-- estimation calculation for the final result using the single and aggregate markers 
select 	estimation_final.*,
		case 
			-- STEP THREE : FR06  USE THE CALCULATED MARKER VALUE OR THE AGGREGATE MARKER VALUE
			when 	agg_marker_cap_val_est > fr06_forward_sales_value and FR06_Logic_Group = 'FR-06 Scenario 1: Middle Sales' 
					OR
					agg_marker_cap_val_est < fr06_prev_sales_value and FR06_Logic_Group = 'FR-06 Scenario 1: Middle Sales' 
					then fr06_data  
					
			when    agg_marker_cap_val_est < fr06_prev_two_sales_value and FR06_Logic_Group = 'FR-06 Scenario 2a: Middle Sales+'
					OR
					agg_marker_cap_val_est > fr06_forward_sales_value and FR06_Logic_Group = 'FR-06 Scenario 2a: Middle Sales+'
					then fr06_data
					
			when    agg_marker_cap_val_est > fr06_forward_two_sales_value and FR06_Logic_Group = 'FR-06 Scenario 2b: Middle Sales+'
					OR
					agg_marker_cap_val_est < fr06_prev_sales_value and FR06_Logic_Group = 'FR-06 Scenario 2b: Middle Sales+'
					then fr06_data
					
			when  	FR06_Logic_Group in ('FR-06 Scenario Out of Scope' , 'FR-06 Scenario 3: Final Sale', 'FR-06 Scenario 4: Single Sale', 'FR-06 Scenario Unknown')
					then (case when agg_marker_cap_val_est=0.0000000 then fr07_data_cv_pc_incr else  agg_marker_cap_val_est end)
					
			when    agg_marker_cap_val_est > fr06_forward_sales_value and FR06_Logic_Group = 'FR-06 Scenario 5: Second Sale' 
					then fr06_data

			when    agg_marker_cap_val_est > fr06_forward_sales_value and FR06_Logic_Group = 'FR-06 Scenario 6: Forward Sale' 
					then fr06_data
			
			when    agg_marker_cap_val_est < fr06_prev_sales_value and FR06_Logic_Group = 'FR-06 Scenario 7: Previous Sale' 
					then fr06_data	
					
					else (case when agg_marker_cap_val_est=0.0000000 then fr07_data_cv_pc_incr else  agg_marker_cap_val_est end) 
					end as final_estimation_data
										
from (
	select      base_outer.* 

				--FR-06 calculations
				--FR-06 Scenario Out of Scope , FR-06 Scenario 3, FR-06 Scenario 4 will return NULL to FR06 and will result in the use of agg_marker_cap_val_est
				-- STEP THREE : FR06 use CV or the calculated value 
			,   case when FR06_Logic_Group = 'FR-06 Scenario 1: Middle Sales' then 
					-- nested case statement to decide if the cv should be used or the calculation values , (cant use cv + % premium as it get too high)
					-- logic to rather use the CV may not be correct and will reflect wrong values when large CVs are used in the data
					-- #####FR06 LOGIC TO USE THE CV VALUE OR THE CALCULATED MARKER VALUE
					-- update this code to exclude the use of CV's if its a multi sale.
					 ( case when (base_outer.fr06_prev_sales_value + base_outer.fr06_forward_sales_value) /2 > base_outer.fr06_default_cv_value then  (base_outer.fr06_prev_sales_value + base_outer.fr06_forward_sales_value) /2 else base_outer.fr06_default_cv_value end)
					
					 when FR06_Logic_Group = 'FR-06 Scenario 2b: Middle Sales+' and nonnullvalue(base_outer.fr06_prev_two_sales_value)=false then
					 ( case when (base_outer.fr06_prev_sales_value + base_outer.fr06_forward_two_sales_value) /2 > base_outer.fr06_default_cv_value then  (base_outer.fr06_prev_sales_value + base_outer.fr06_forward_two_sales_value) /2 else base_outer.fr06_default_cv_value end)
					 
					 when FR06_Logic_Group = 'FR-06 Scenario 2a: Middle Sales+' and nonnullvalue(base_outer.fr06_forward_two_sales_value)=false then 
					 ( case when (base_outer.fr06_forward_sales_value + base_outer.fr06_prev_two_sales_value) /2 > base_outer.fr06_default_cv_value then  (base_outer.fr06_forward_sales_value + base_outer.fr06_prev_two_sales_value) /2 else base_outer.fr06_default_cv_value end)
					 
					 when   FR06_Logic_Group    = 'FR-06 Scenario 3: Final Sale' 
					 OR     FR06_Logic_Group    = 'FR-06 Scenario 4: Single Sale'
					 OR     FR06_Logic_Group    = 'FR-06 Scenario Out of Scope'
					 OR     FR06_Logic_Group    = 'FR-06 Scenario Unknown'
					 then NULL --this scenario doesn't result in any calculation
					
					 when FR06_Logic_Group = 'FR-06 Scenario 5: Second Sale' then 
					 ( case when  (base_outer.fr06_default_cv_value+ base_outer.fr06_forward_sales_value) /2 > base_outer.fr06_default_cv_value then   (base_outer.fr06_default_cv_value+ base_outer.fr06_forward_sales_value) /2 else base_outer.fr06_default_cv_value end)
					
					 when FR06_Logic_Group = 'FR-06 Scenario 6: Forward Sale' then 
					 ( case when  (base_outer.fr06_default_cv_value+ base_outer.fr06_forward_sales_value) /2 > base_outer.fr06_default_cv_value then   (base_outer.fr06_default_cv_value+ base_outer.fr06_forward_sales_value) /2 else base_outer.fr06_default_cv_value end)
					
					when FR06_Logic_Group = 'FR-06 Scenario 7: Previous Sale' then 
					 ( case when  (base_outer.fr06_default_cv_value+ base_outer.fr06_prev_sales_value) /2 > base_outer.fr06_default_cv_value then   (base_outer.fr06_default_cv_value+ base_outer.fr06_prev_sales_value) /2 else base_outer.fr06_default_cv_value end)
									
					 else NULL -- null will means its scenario 3 or 4 or its an out of scope scenario that will mean using the agg_marker_cap_val_est value instead
					 
					 end as FR06_data
					 --agg_marker_cap_val_est as agg_marker_cap_val_est_debug -- remove this debug column later

	-- the logic here is the create acceptable ranges of the actual values. 
			,   case    when Inferred_avg_medi_diff_pc >= ${fr07_range_start=60,65,70,75,80} and Inferred_avg_medi_diff_pc   <= ${fr07_range_end=140,135,130,125,120} and sales_value != 0.00 then '[1] Acceptable Inferred Value'
						when Inferred_avg_medi_diff_pc < ${fr07_range_start} and sales_value != 0.00  then '[2] Inferred Value Too High'
						when Inferred_avg_medi_diff_pc > ${fr07_range_end} and  sales_value != 0.00  then '[3] Inferred Value Too Low' else 'Unknown'end as accuracy_category
			,   replace(NVL(concat(cast( Inferred_avg_medi_diff_pc as  string), '%'),'Unknown'),'0.0%','Unknown' )accuracy_of_estimate
	from (

		--## above works on accuracy estimations
		Select 
			  base_inner.property_qpid
			, base_inner.property_title_no
			, base_inner.property_address
			, base_inner.property_suburb
			, base_inner.property_town
			, base_inner.property_land_area_sml_cat 
			, base_inner.property_land_zone_desc
			, base_inner.property_category_desc
			, base_inner.property_land_use_desc
			, base_inner.property_build_age_group
			, base_inner.property_build_age_act
			, base_inner.property_residential_indicator
			, base_inner.property_transmission_sale_ind
			, base_inner.property_building_floor_area
			, base_inner.sale_type_code
			, base_inner.Settlement_month
			, base_inner.Settlement_year
			, base_inner.Settlement_date
			, base_inner.Sales_month
			, base_inner.Sales_year
			, base_inner.Sales_date
			, base_inner.fr06_prev_two_sales_value
			, base_inner.fr06_prev_sales_value
			, base_inner.Sales_value 
			, base_inner.fr06_forward_sales_value
			, base_inner.fr06_forward_two_sales_value

			-- FR06_Logic may have to move to base outer to handle the agg_marker_cap_val_est comparison to prev sale and forward sale > <
			-- STEP TWO : FR06 (this could be within the previous select as to "reduce" a sub-select ) put the sale record into the FR06 category based on the logic proposed
			, case 
				when  nonnullvalue(base_inner.fr06_prev_two_sales_value)=false and base_inner.fr06_prev_sales_value > 0 and base_inner.Sales_value =0 and base_inner.fr06_forward_sales_value > 0 
				then 'FR-06 Scenario 1: Middle Sales' 
				
				when base_inner.fr06_prev_two_sales_value > 0 and base_inner.fr06_prev_sales_value =0 and base_inner.Sales_value = 0 and base_inner.fr06_forward_sales_value >0 and nonnullvalue(base_inner.fr06_forward_two_sales_value)=false 
				then 'FR-06 Scenario 2a: Middle Sales+' 
				
				when base_inner.fr06_prev_sales_value >0 and base_inner.Sales_value =0 and base_inner.fr06_forward_sales_value = 0 and base_inner.fr06_forward_two_sales_value > 0 and nonnullvalue(base_inner.fr06_prev_two_sales_value)=false  
				then 'FR-06 Scenario 2b: Middle Sales+' 
				
				when nonnullvalue(base_inner.fr06_prev_two_sales_value)=false  and base_inner.fr06_prev_sales_value >0 and base_inner.Sales_value = 0 and nonnullvalue(base_inner.fr06_forward_two_sales_value)=false 
				then 'FR-06 Scenario 3: Final Sale' 
				
				when nonnullvalue(base_inner.fr06_prev_sales_value)=false and base_inner.Sales_value = 0 and nonnullvalue(base_inner.fr06_forward_sales_value)=false 
				then 'FR-06 Scenario 4: Single Sale' 
				
				when nonnullvalue(base_inner.fr06_prev_sales_value)=false and base_inner.Sales_value = 0 and base_inner.fr06_forward_sales_value > 0 and nonnullvalue(base_inner.fr06_forward_two_sales_value)=false 
				then 'FR-06 Scenario 5: Second Sale'
				
				when base_inner.fr06_prev_two_sales_value=0 and base_inner.fr06_prev_sales_value=0 and base_inner.Sales_value = 0 and base_inner.fr06_forward_sales_value > 0   -- there is no limit on more sales in future or in the past past 2 previous sales
				OR nonnullvalue(base_inner.fr06_prev_two_sales_value)=false and base_inner.fr06_prev_sales_value=0 and base_inner.Sales_value = 0 and base_inner.fr06_forward_sales_value > 0 
				then 'FR-06 Scenario 6: Forward Sale'
				
				when base_inner.fr06_forward_two_sales_value=0 and base_inner.fr06_forward_sales_value=0 and base_inner.Sales_value = 0 and base_inner.fr06_prev_sales_value > 0   -- there is no limit on more sales in previous or in the future past 2 future sales
				OR nonnullvalue(base_inner.fr06_forward_two_sales_value)=false and base_inner.fr06_forward_sales_value=0 and base_inner.Sales_value = 0 and base_inner.fr06_prev_sales_value > 0 
				then 'FR-06 Scenario 7: Previous Sale'
				
				when base_inner.Sales_value > 0 then 'FR-06 Scenario Out of Scope'
				
				else 'FR-06 Scenario Unknown'

			 end as FR06_Logic_Group
	 
			--FR07 Logic ------
			, nvl(
					case --essentially we will select the CV value + 21% if the sales where within [x] days of the revision CV value
						when base_inner.cv_over_period_of_sale=1 AND base_inner.Overarching_inferred_calc < base_inner.Sales_Capital_Value+(base_inner.Sales_Capital_Value * ${fr07_percentage_cv_increase=5,10,15,20,21,22,23,24,25,26} / 100.0)
						then base_inner.Sales_Capital_Value+(base_inner.Sales_Capital_Value * ${fr07_percentage_cv_increase=5,10,15,20,21,22,23,24,25,26} / 100.0)
						
						when base_inner.cv_over_period_of_sale=0 and base_inner.Sales_Capital_Value < 3500000  and base_inner.Sales_Capital_Value >0 AND base_inner.Overarching_inferred_calc > base_inner.Sales_Capital_Value 
						then base_inner.Sales_Capital_Value+(base_inner.Sales_Capital_Value * ${fr07_percentage_cv_increase=5,10,15,20,21,22,23,24,25,26} / 100.0)
					else  base_inner.Overarching_inferred_calc  -- adds % to the when choosing the CV value
			  
			  end,0) as agg_marker_cap_val_est -- here is the aggregate marker that looks at averages/medians to output an average between those two and will be used to calculate against FR-06
			  
			, round(base_inner.Sales_value * 100.0 / case 
															when base_inner.cv_over_period_of_sale=1 AND base_inner.Overarching_inferred_calc < base_inner.Sales_Capital_Value+(base_inner.Sales_Capital_Value * ${fr07_percentage_cv_increase=5,10,15,20,21,22,23,24,25,26} / 100.0)
															then base_inner.Sales_Capital_Value+(base_inner.Sales_Capital_Value * ${fr07_percentage_cv_increase=5,10,15,20,21,22,23,24,25,26} / 100.0)
															
															when base_inner.cv_over_period_of_sale=0 and base_inner.Sales_Capital_Value < 3500000   and base_inner.Sales_Capital_Value >0 AND base_inner.Overarching_inferred_calc > base_inner.Sales_Capital_Value 
															then base_inner.Sales_Capital_Value+(base_inner.Sales_Capital_Value * ${fr07_percentage_cv_increase} / 100.0)
															
															else  base_inner.Overarching_inferred_calc  -- adds % to the when choosing the CV value
													 end, 
				1)  as Inferred_avg_medi_diff_pc

			, base_inner.Sales_inferred_averages
			, base_inner.Sales_inferred_Median
			, base_inner.Sales_inferred_averages_res
			, base_inner.Sales_inferred_Median_res
			, base_inner.fr06_default_cv_value
			, base_inner.Sales_Capital_Value 
			, base_inner.Sales_Capital_Value+(base_inner.Sales_Capital_Value * ${fr07_percentage_cv_increase} / 100.0) as fr07_data_cv_pc_incr  -- adds 21% to the when choosing the CV value
			, base_inner.cv_val_lookup
			, base_inner.cv_rev_date_lookup
			, base_inner.cv_upd_date_lookup
			, base_inner.days_diffrence_cv_sales
			, base_inner.cv_over_period_of_sale
			from
			(
				select distinct 
					  base.property_qpid
					, base.property_title_no
					, base.property_address
					, base.property_suburb
					, base.property_town
					, base.property_land_area_sml_cat 
					, base.property_land_zone_desc
					, base.property_category_desc
					, base.property_land_use_desc
					, base.property_building_floor_area
					, base.property_build_age_group
					, base.property_build_age_act
					, base.property_residential_indicator
					, base.property_transmission_sale_ind
					, base.sale_type_code
        			, base.Settlement_month
        			, base.Settlement_year
        			, base.Settlement_date
					, base.Sales_month
					, base.Sales_year
					, base.Sales_date
					, base.Sales_value
					, base.fr06_prev_sales_value
					, base.fr06_forward_sales_value
					, base.fr06_prev_two_sales_value
					, base.fr06_forward_two_sales_value
					-- logic start for marker selection to use which marker from which bucket 
					-- update the below to have in the case statement a check to see if the record is residential / BL then use the Sales_inferred_averages_res_bl and Sales_inferred_Median_res_bl
					
					-- ESTIMATION LOGIC ON TOP OF AGGREGATE MARKERS TO GE TTHE AVERAGE OF THE TWO MARKERS FOR RES AND 
					, case  when base.property_residential_indicator != 'Residential' 
							then round(round(isv2.sales_median +isv.sales_averages ,2)/2,2) -- use the bucket averages for 
						else round(round(isv3.sales_averages_res +isv4.sales_median_res ,2)/2,2) 
						end as Overarching_inferred_calc
					, round(base.Sales_value * 100.0 / round(round(isv2.sales_median +isv.sales_averages ,2)/2,2), 1)  as Debug_overarching_inferred_calc_diff_pc
					--logic end for marker selection 
					
					, isv.sales_averages as Sales_inferred_averages
					, isv2.sales_median as Sales_inferred_Median
					, isv3.sales_averages_res as Sales_inferred_averages_res
					, isv4.sales_median_res as Sales_inferred_Median_res
					, base.sale_cv as Sales_Capital_Value 
					, case when base.sale_type_code NOT IN ('M11','M12','M13','M21','M22','M23','M31','M32','M33','M43') then base.sale_cv else 200000.00 end as fr06_default_cv_value -- we have a default as to not select a CV that is far too high in code above in fr06 step 3
					, base.cv_val_lookup
					, base.cv_rev_date_lookup
					, base.cv_upd_date_lookup
					,  case 
					  
					--i just want days not negative values so translate to all positive numbers
						 when  cast(replace(cast(datediff(base.Sales_date,base.cv_rev_date_lookup)as string),'-','')as int)  > ${fr07_days_between_sale_cv=16,32,36,42,85,126,136,148,164} and base.cv_rev_date_lookup is not null
						 then 1 
						 
						 when  cast(replace(cast(datediff(base.Sales_date,base.cv_rev_date_lookup)as string),'-','')as int)  > ${fr07_days_between_sale_cv=16,32,36,42,85,126,136,148,164} and base.cv_rev_date_lookup is null OR   base.cv_rev_date_lookup is null
						 then 2-- we account for no dates to make this calculation when 2 we wont use the cv+%
						 
						 else 0 
					  end as cv_over_period_of_sale  --136    
					, cast(replace(cast(datediff(base.Sales_date,base.cv_rev_date_lookup)as string),'-','')as int)  as days_diffrence_cv_sales
					from 
						( -- includes cv values (stand alone not aggregated)
					--#7  
					select base_zero.*,
									-- looking at previous sales values for property
									-- when there is duplicates in the sales records, then we return incorrect figures, this is a data bug that messes up the logic
									-- STEP ONE : FR06 get the dates for past and forward sales from the sale with no sales value that needs a value estimate.
									----------------------------------
									lag(base_zero.Sales_value,1) 
									OVER (partition by base_zero.property_qpid, base_zero.property_title_no, base_zero.property_address  ORDER BY base_zero.property_qpid, base_zero.property_title_no, base_zero.property_address, base_zero.Sales_date ) as fr06_prev_sales_value,
									
									lead(base_zero.Sales_value,1) 
									OVER (partition by base_zero.property_qpid, base_zero.property_title_no, base_zero.property_address  ORDER BY base_zero.property_qpid, base_zero.property_title_no, base_zero.property_address,  base_zero.Sales_date ) as fr06_forward_sales_value,

									lag(base_zero.Sales_value,2) 
									OVER (partition by base_zero.property_qpid, base_zero.property_title_no , base_zero.property_address ORDER BY base_zero.property_qpid, base_zero.property_title_no, base_zero.property_address, base_zero.Sales_date ) as fr06_prev_two_sales_value,
									
									lead(base_zero.Sales_value,2) 
									OVER (partition by base_zero.property_qpid, base_zero.property_title_no, base_zero.property_address ORDER BY base_zero.property_qpid, base_zero.property_title_no, base_zero.property_address, base_zero.Sales_date ) as fr06_forward_two_sales_value

									----------------------------------
						from (
							--##### PROPERTIES #####
							select  --distinct -- this distinct was needed to stop duplicates REMOVED distinct as part of DIPPRP-129 (may need a sub query to add it)
									cls.qpid as property_qpid,
									pt.title_title_no as property_title_no,
									pt.property_address, 
									pt.property_suburb, 
									pt.property_town,  
									pt.property_land_area_sml_cat, 
									pt.property_land_zone_desc,
									pt.property_category_desc,
									pt.property_land_use_desc,
									pt.property_building_floor_area,
									pt.property_residential_indicator,
									pt.property_build_age_group,
									pt.property_build_age_act,
									NVL(transmission_excl_records.property_transmission_sale_ind,'Unknown') as property_transmission_sale_ind, -- added for 0.5v essentially these didn't join via the dates
									cls.sale_cv,
									cls.sale_type_code,
									pv.capital_value as cv_val_lookup,
									MAX(pv.revision_date)  as cv_rev_date_lookup,
									pv.update_date  as cv_upd_date_lookup,
									cls.settlement_date,
									MONTHNAME(cls.settlement_date) as Settlement_month , 
									date_part('Year',cls.settlement_date) as Settlement_year,
									MONTHNAME(cls.sale_date) as Sales_month , 
									date_part('Year',cls.sale_date) as Sales_year,
									cls.sale_date as Sales_date ,
									cls.sale_price_gross  as Sales_value
									
									from lab_fcp_raw_restricted_access.pty_cl_sale  cls -- we want to look at the latest sale values for the property
									
									-- add additional property data to the sale latest record using the qpid join       
									INNER JOIN lab_property_project.tmp_pty_l_cl_new_property pt
									on pt.property_qpid = cls.qpid 
									

									left outer join 
									
									(
										 -- transmission sales exclusion list Added 20_04 as bright-line sales do not include transmissions and need a flag for this 
										 -- to exclude  
										select  
											case when 
													trt_type 
													in ('TSFL','TSM','TSMM') 
													then 'Yes' else 'No' 
											end as property_transmission_sale_ind
										,	cast(to_date(ti.lodged_datetime) as timestamp) as instrument_trans_date
										,   tin.ttl_title_no 
										from   cp_pty_l_title_instrument ti 
										inner join   cp_pty_l_title_instrument_title tin on tin.tin_id= ti.id
										where  trt_type in ('TSFL','TSM','TSMM')
									) as transmission_excl_records ON 
									transmission_excl_records.ttl_title_no = pt.title_title_no and
									transmission_excl_records.instrument_trans_date = cls.settlement_date
									
									
									
									/* REMOVED AS PART OF DIPPRP-118   replaced by pt.property_build_age_group
									-- add the building age
									left outer join  lab_fcp_raw_restricted_access.pty_cl_property clpt
									on clpt.qpid = pt.property_qpid */
									
									-- add the valuation record to see if the CV was done around the same time 
									left outer join cp_pty_cl_valuation pv 
									ON pv.qpid= cls.qpid  and pv.capital_value=cls.sale_cv
									
									-- Added as part of a fix to reduce duplicates raise in Jira DIPPRP-129
									GROUP BY 
										cls.qpid 
								    ,	pt.title_title_no 
									,   pt.property_address 
									,   pt.property_suburb
								    ,   pt.property_town 
									,   pt.property_land_area_sml_cat
									,   pt.property_land_zone_desc
									,   pt.property_category_desc
									,   pt.property_land_use_desc
									,   pt.property_building_floor_area
									,   pt.property_residential_indicator
									, 	transmission_excl_records.property_transmission_sale_ind  -- added 20_04_2020
									,   pt.property_build_age_group
									,   pt.property_build_age_act
									,   cls.sale_cv
									,   cls.sale_type_code
									,   pv.capital_value 
									,   pv.update_date 
									,   cls.settlement_date
									,   MONTHNAME(cls.settlement_date) 
									,   date_part('Year',cls.settlement_date) 
									,   MONTHNAME(cls.sale_date) 
								    ,  	date_part('Year',cls.sale_date) 
									,   cls.sale_date 
									,   cls.sale_price_gross  
								) as base_zero

						) as base  
						
						-- #####AGGREGATE MARKERS:##### inferred averages yearly by suburb and area
						-- Bucket 1.1 and 1.2: Aggregate Markers based on property only within any sales period
						LEFT OUTER JOIN 
						(
							select  property_build_age_group, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year, avg(sales_value) as sales_averages
							from 
							(
								select cls.qpid as property_qpid,
								pt.property_address, 
								pt.property_suburb, 
								pt.property_town,  
								pt.property_land_area_sml_cat,
								pt.property_land_zone_desc,
								pt.property_build_age_group,
								MONTHNAME(cls.sale_date) as Sales_month , 
								date_part('Year',cls.sale_date) as Sales_year,
								cls.sale_price_gross   as Sales_value,
								cls.sale_cv
								from lab_fcp_raw_restricted_access.pty_cl_sale  cls
								
								-- add additional property data to the sale latest record using the qpid join
								INNER JOIN lab_property_project.tmp_pty_l_cl_new_property pt
								on pt.property_qpid = cls.qpid 
								
								/* REMOVED AS PART OF DIPPRP-118   replaced by pt.property_build_age_group
								-- add the building age
								left outer join  lab_fcp_raw_restricted_access.pty_cl_property clpt
								on clpt.qpid = pt.property_qpid */
								
								WHERE  cls.sale_cv < 3500000 -- removes CV's that skew calculation averages/ medians
								AND cls.sale_type_code NOT IN ('M11','M12','M13','M21','M22','M23','M31','M32','M33','M43') -- MULTI SALES
							   -- AND pt.property_land_zone_desc LIKE 'Residential%' -- only do the calculations on residential property
							) as area_averages
							where sales_value != 0 and sales_value is not null-- exclude properties with no sales values in the averages (the multiple instruments issue per sale might be an issue)
							AND sales_value < 3500000 -- excludes properties higher than 10mil that skew the averages these are mos tof the time buld land purchese for high value $$
							
							--##### CATEGORIES#####
							group by property_build_age_group, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year
						   
						) as isv
						-- #####CONNECTIONS TO PROPERTY#####
						ON  isv.Property_suburb=base.Property_suburb
						AND isv.property_town=base.property_town
						AND isv.property_land_area_sml_cat=base.property_land_area_sml_cat
						AND isv.property_land_zone_desc=base.property_land_zone_desc
						AND isv.sales_year=base.sales_year
						AND isv.property_build_age_group=base.property_build_age_group
						
						-- #####AGGREGATE MARKERS:##### inferred median sales yearly by suburb and area
						-- Bucket 1.1 and 1.2: Aggregate Markers based on property only within any sales period
						LEFT OUTER JOIN 
						(
						select property_build_age_group, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year, appx_median(sales_value)  as sales_median
							from 
							(
								select cls.qpid as property_qpid,
								pt.property_address, 
								pt.property_suburb, 
								pt.property_town,  
								pt.property_land_area_sml_cat,
								pt.property_land_zone_desc,
								pt.property_build_age_group,
								MONTHNAME(cls.sale_date) as Sales_month , 
								date_part('Year',cls.sale_date) as Sales_year,
								cls.sale_price_gross   as Sales_value,
								cls.sale_cv
								from lab_fcp_raw_restricted_access.pty_cl_sale  cls
								
								-- add additional property data to the sale latest record using the qpid join
								INNER JOIN lab_property_project.tmp_pty_l_cl_new_property pt
								on pt.property_qpid = cls.qpid 
								
								/* REMOVED AS PART OF DIPPRP-118   replaced by pt.property_build_age_group
								-- add the building age
								left outer join  lab_fcp_raw_restricted_access.pty_cl_property clpt
								on clpt.qpid = pt.property_qpid */
									
								WHERE  cls.sale_cv < 3500000  -- removes CV's that skew calculation averages/ medians
								AND cls.sale_type_code NOT IN ('M11','M12','M13','M21','M22','M23','M31','M32','M33','M43') -- MULTI SALES
								) as area_medians
							where sales_value != 0 and sales_value is not null -- exclude properties with no sales values in the averages (the multiple instruments issue per sale might be an issue)
							AND sales_value < 3500000 -- excludes properties higher than 10mil that skew the averages these are most of the time build land purchase for high value $$
							
							--##### CATEGORIES#####
							group by property_build_age_group, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year
						) as isv2
						
						-- #####CONNECTIONS TO PROPERTY#####
						ON  isv2.Property_suburb=base.Property_suburb
						AND isv2.property_town=base.property_town
						AND isv2.property_land_area_sml_cat=base.property_land_area_sml_cat
						AND isv2.property_land_zone_desc=base.property_land_zone_desc
						AND isv2.sales_year=base.sales_year
						AND isv2.property_build_age_group=base.property_build_age_group

					 -- #####AGGREGATE MARKERS:##### inferred residential bright line sales average MARKER yearly by suburb and area
						LEFT OUTER JOIN 
						(
						select property_build_age_group, property_residential_indicator, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year, avg(sales_value)  as sales_averages_res
							from 
							(
								select cls.qpid as property_qpid,
								pt.property_address, 
								pt.property_suburb, 
								pt.property_town,  
								pt.property_land_area_sml_cat,
								pt.property_land_zone_desc,
								pt.property_residential_indicator,
								pt.property_build_age_group,
								MONTHNAME(cls.sale_date) as Sales_month , 
								date_part('Year',cls.sale_date) as Sales_year,
								cls.sale_price_gross   as Sales_value,
								cls.sale_cv
								from lab_fcp_raw_restricted_access.pty_cl_sale  cls
								
								-- add additional property data to the sale latest record using the qpid join
								INNER JOIN lab_property_project.tmp_pty_l_cl_new_property pt
								on pt.property_qpid = cls.qpid 
								
								/* REMOVED AS PART OF DIPPRP-118   replaced by pt.property_build_age_group
								-- add the building age
								left outer join  lab_fcp_raw_restricted_access.pty_cl_property clpt
								on clpt.qpid = pt.property_qpid */
								
								WHERE  cls.sale_cv < 3500000  -- removes CV's that skew calculation averages/ medians
								AND pt.property_residential_indicator= 'Residential' -- logic already applied to column but needs and update    
								AND cls.sale_type_code NOT IN ('M11','M12','M13','M21','M22','M23','M31','M32','M33','M43') -- MULTI SALES
								) as area_brightlines
							where sales_value != 0 and sales_value is not null -- exclude properties with no sales values in the averages (the multiple instruments issue per sale might be an issue)
							AND sales_value < 3500000 -- excludes properties higher than 10mil that skew the averages these are mos tof the time buld land purchese for high value $$
							
							--#####CATEGORIES#####
							group by property_build_age_group, property_residential_indicator, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year
						) as isv3
						
						-- #####CONNECTIONS TO PROPERTY#####
						ON  isv3.Property_suburb=base.Property_suburb
						AND isv3.property_town=base.property_town
						AND isv3.property_land_area_sml_cat=base.property_land_area_sml_cat
						AND isv3.property_land_zone_desc=base.property_land_zone_desc
						AND isv3.sales_year=base.sales_year
						AND isv3.property_residential_indicator=base.property_residential_indicator
						AND isv3.property_build_age_group=base.property_build_age_group
					 
						--#####AGGREGATE MARKERS:##### inferred residential bright line sales median MARKER yearly by suburb and area
						LEFT OUTER JOIN 
						(
						select property_build_age_group, property_residential_indicator, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year, appx_median(sales_value)  as sales_median_res
							from 
							(
								select cls.qpid as property_qpid,
								pt.property_address, 
								pt.property_suburb, 
								pt.property_town,  
								pt.property_land_area_sml_cat,
								pt.property_land_zone_desc,
								pt.property_residential_indicator,
								pt.property_build_age_group, -- DIPPRP-118
								MONTHNAME(cls.sale_date) as Sales_month , 
								date_part('Year',cls.sale_date) as Sales_year,
								cls.sale_price_gross   as Sales_value,
								cls.sale_cv
								from lab_fcp_raw_restricted_access.pty_cl_sale  cls
								-- add additional property data to the sale latest record using the qpid join
								
								INNER JOIN lab_property_project.tmp_pty_l_cl_new_property pt -- pre-generated table
								on pt.property_qpid = cls.qpid 
								
								/* REMOVED AS PART OF DIPPRP-118   replaced by pt.property_build_age_group
								-- add the building age
								left outer join  lab_fcp_raw_restricted_access.pty_cl_property clpt
								on clpt.qpid = pt.property_qpid */
								
								WHERE  cls.sale_cv < 3500000  -- removes CV's that skew calculation averages/ medians
								AND pt.property_residential_indicator= 'Residential' -- logic already applied to column but needs and update    
								AND cls.sale_type_code NOT IN ('M11','M12','M13','M21','M22','M23','M31','M32','M33','M43') -- MULTI SALES
								) as area_brightlines
							where sales_value != 0 and sales_value is not null -- exclude properties with no sales values in the averages (the multiple instruments issue per sale might be an issue)
							AND sales_value < 3500000 -- excludes properties higher than 10mil that skew the averages these are mos tof the time buld land purchese for high value $$
							
							--#####CATEGORIES#####
							group by property_build_age_group, property_residential_indicator, property_land_zone_desc, property_land_area_sml_cat, Property_suburb, property_town, sales_year
						) as isv4
						
						-- #####CONNECTIONS TO PROPERTY#####
						ON  isv4.Property_suburb=base.Property_suburb
						AND isv4.property_town=base.property_town
						AND isv4.property_land_area_sml_cat=base.property_land_area_sml_cat
						AND isv4.property_land_zone_desc=base.property_land_zone_desc
						AND isv4.sales_year=base.sales_year
						AND isv4.property_residential_indicator=base.property_residential_indicator
						AND isv4.property_build_age_group=base.property_build_age_group
							) as base_inner 
					 
					   ) as base_outer --and fr06 is not null-- FR06_Logic like 'FR-06 Scenario 1' --or FR06_Logic='FR-06 Scenario 5'-- and sales_value 0
					) as estimation_final  --where sales_value>0 --FR06_Logic_group like  'FR-06 Scenario 2%' 
					
					--where property_address='116/26 Te Taou Cres' and property_title_no='686211' and sales_year=2009
					) as estimation_quality --where property_title_no= '441937'
					--  duplicates i this sale record? where property_title_no = 'CB501/128'
					where FR06_Logic_Group != 'FR-06 Scenario Out of Scope' -- this where clause ensure we only return estimations for the 0 sales values
				--	 and 
				--	property_title_no in ('SA48D/471')
					--('632481', '696181', '710605', '698146', 'WN53B/654', 'NA85C/838',
					--'CB3C/535','747210','WN34D/805','SA1051/99', '113182','784876','NA133A/884','SA1776/7B','TNG4/383')
					
					-- and FR06_Logic_Group = 'FR-06 Scenario 1: Middle Sales' 
				--	)as count_rec group by quality_estimation_level
					/*and FR06_Logic_Group = ${FR06_Logic_Group='FR-06 Scenario 1: Middle Sales' ,
																'FR-06 Scenario 2a: Middle Sales+', 
																'FR-06 Scenario 2b: Middle Sales+', 
																'FR-06 Scenario 3: Final Sale',
																'FR-06 Scenario 4: Single Sale',
																'FR-06 Scenario 5: Second Sale',
																'FR-06 Scenario 6: Forward Sale',
																'FR-06 Scenario 7: Previous Sale',
																'FR-06 Scenario Unknown'}*/
																
															--	)as count_rec group by quality_estimation_level --FR06_Logic_Group
					--limit ${Limit_Records=25,50,100,200,500,100000, 200000}
					/*
					 --  FR-06 Scenario Out of Scope , FR-06 Scenario 3, FR-06 Scenario 4 will return NULL to FR06 and will result in the use of agg_marker_cap_val_est
					-- ## base data from here lab_fcp_raw_restricted_access
					  ) as count_ 
					   group by accuracy_category order by accuracy_category_count desc --#3
					 ) as estimate group by accuracy_category_pass_val--#2
			
					 ) as estimate_final --#1*/
					 

					 
