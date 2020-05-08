   /*
   
   	Author: Mark Openstein
	Overview: Determines if the issue date of a title (new) is within a financial year period for a property.  
			  The query results in a table that has the many-to-many relationship data between qpids and titles e.g. some data will have the same title with many associated qpids, or one qpid can have many associated titles.
			  The query uses an inner join on the max update_date of the property record to reduce duplicate values
	
	Source: Linz and CoreLogic	
	
	Gary's requests addressed: 
	
	1. Number of residential properties in New Zealand (maybe under a certain size to rule out farmland)
	2. Number of new properties developed in the last 12 months (based on title issue date, financial year) 
	3. Number of Types of new developments – stand-alone housing vs apartments 
	4. Comparison to the number of properties developed in the previous 12 months
	
	Assumptions: 
	
	a. A "New development", "New property" in the query is based on the title as per Chris, (however you could theoretically have title for a piece of land issued in 2005 and have a new property apartment block developed in 2019, "Consent"?)
	b. A title can have many QPIDS and a QPID can have many titles (Many-to-Many)


--********FOR REVIEW********** 
--this example presents a data quality issue when doing counts of properties in the PD schema and highlights the M2M relationship between qpids and titles.
--example of the many to many data between titles and qpids
--select * from tmp_pty_l_cl_fin_new_prop where property_qpid= 2202758
--select * from pty_pd_property_history where qpid= 2202758 and current_flag='Y'	

-- IMPORTANT INFO: Leave the variable values blank in HUE and they will run as if they didnt exist (remember to blank iout the table create).
-- This table is currently used as part of Option 7 for the estimation merging code.
	Versions:
	-----------
	-- 1.0 Added some comments 7/05/2020
	-- 0.9 lab_fcp_raw_restricted_access source data updated to get from new schema DIPPRP-107
	-- 0.9 property_build_age_group added (moved logic from the estimation logic code to this table) DIPPRP-118
	-- 0.8 added a filter for Quality match =2 only.!!!!this is to select correct addresses but does limit data returned DIPPRP-116
        -- 0.7 added time share indicator - i don't know how this works over time, if you had a timeshare then that got removed??? 
 	-- the code essentially just rolls up an indicator if there are timeshares. - may need to look at the YN column called original_flag and only inlcude Y records?
	 0.1-0.2 -  1. consent logic updated
				2. time period added for dwg consent dates
				3. Added lab_property_project.tmp_pty_imp_fin_yoy_ndwg_prop
				4. removed columns and some additional comments added
		0.3 -
				1. Added complex sub queries to get a distinct multi builders information for the consent date
		0.4 	1. bug with FY flags
		0.5 	1. Residential Properties BY OWNER ENTITY
		0.6: 	1. added property_residential_indicator
   */


create table lab_property_project.tmp_pty_l_cl_new_property --< maybe we need a new replacement for tmp_ prefix if used in a "prod" etl process
STORED AS PARQUET AS


select * from ( --<< generally shouldnt have select * from anything.
select distinct 
      all_property_dates.title_title_no
    , all_property_dates.title_issue_date
    , case when all_property_dates.property_timeshare_indicator = 1 then 'Y' else 'N' end as property_timeshare_indicator -- timeshare logic
	, all_property_dates.property_residential_indicator
    , all_property_dates.property_category_code
    , all_property_dates.property_category_desc
    , all_property_dates.property_land_zone_code
    , all_property_dates.property_land_zone_desc
    , all_property_dates.property_land_smpl_zne_desc
    , all_property_dates.property_land_use_code
	, all_property_dates.property_land_use_desc
    , all_property_dates.property_building_floor_area
    , all_property_dates.property_land_area
    , all_property_dates.property_land_area_sml_cat 
    , all_property_dates.property_land_area_lrg_cat
    , all_property_dates.property_qpid
    , ISNULL(all_property_dates.property_building_name,'Unknown') as property_building_name
	, all_property_dates.property_build_age_group
	, all_property_dates.property_build_age_act
    , all_property_dates.property_address
    , all_property_dates.property_suburb
    , all_property_dates.property_town
    , all_property_dates.property_postcode
	, ISNULL(all_property_dates.property_consent_lat_iss_date,cast('9000-12-31 00:00:00' as timestamp)) as property_consent_lat_iss_date
	, ISNULL(all_property_dates.property_consent_new_dwg_flag,'X') as property_consent_new_dwg_flag
	, ISNULL(all_property_dates.property_consent_builders,'Unknown') as property_consent_builders
    , max_property_date.property_max_update_date
    , all_property_dates.cal_fin_ttl_year_16_17_prpty_flag /*static periods, may be done in VIYA ##can be removed##*/
    , all_property_dates.cal_fin_ttl_year_17_18_prpty_flag /*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.cal_fin_ttl_year_18_19_prpty_flag /*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.cal_fin_ttl_year_19_20_prpty_flag /*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.last_12_mnths_ttl_prpty_flag  		/*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.cal_fin_year_16_17_ndwg_flag /*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.cal_fin_year_17_18_ndwg_flag /*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.cal_fin_year_18_19_ndwg_flag /*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.cal_fin_year_19_20_ndwg_flag 	/*static periods, may be done in VIYA##can be removed##*/	
    , all_property_dates.last_12_mnths_ndwg_flag /*static periods, may be done in VIYA##can be removed##*/
    , all_property_dates.extraction_run_date 
    , all_property_dates.extraction_run_date_minus_12_mnths /*static periods, may be done in VIYA##can be removed##*/
 from ( 
     select 	  
          title_title_no
        , title_issue_date
        , property_category_code
        , property_category_desc
        , property_land_zone_code
        , property_land_smpl_zne_desc
        , property_land_zone_desc
        , property_land_use_code
		, property_land_use_desc
        , property_building_floor_area
        , property_land_area
        , property_land_area_sml_cat 
        , property_land_area_lrg_cat
        , property_qpid
        , property_building_name
		, property_build_age_group
		, property_build_age_act
        , property_address
        , property_suburb
        , property_town
        , property_postcode 
        , property_update_date
		, property_consent_lat_iss_date
		, property_consent_new_dwg_flag
		, property_consent_builders
		, property_residential_indicator
        , MAX(property_timeshare_indicator_stg) as property_timeshare_indicator-- timeshare logic
        , cal_fin_ttl_year_16_17_prpty_flag/*##can be removed##*/
        , cal_fin_ttl_year_17_18_prpty_flag /*##can be removed##*/
        , cal_fin_ttl_year_18_19_prpty_flag /*##can be removed##*/
        , cal_fin_ttl_year_19_20_prpty_flag /*##can be removed##*/
        , last_12_mnths_ttl_prpty_flag 	/*##can be removed##*/	
        , cal_fin_year_16_17_ndwg_flag/*##can be removed##*/
        , cal_fin_year_17_18_ndwg_flag /*##can be removed##*/
        , cal_fin_year_18_19_ndwg_flag /*##can be removed##*/
        , cal_fin_year_19_20_ndwg_flag /*##can be removed##*/		
        , last_12_mnths_ndwg_flag /*##can be removed##*/
        , extraction_run_date/*##can be removed##*/
        , extraction_run_date_minus_12_mnths
     from ( 
        select 
		  title_title_no
        , title_issue_date
        , case when property_timeshare_week_no is null then 0 else 1 end as property_timeshare_indicator_stg-- timeshare logic
        , property_category_code
        , property_category_desc
        , property_land_zone_code
        , property_land_zone_desc
        , property_land_smpl_zne_desc
        , property_land_use_code
		, property_land_use_desc
        , property_building_floor_area
        , property_land_area
        , property_land_area_sml_cat 
        , property_land_area_lrg_cat
        , property_qpid
        , property_building_name
		, property_build_age_group
		, property_build_age_act
        , property_address
        , property_suburb
        , property_town
        , property_postcode
        , property_update_date
		, property_consent_lat_iss_date
		, property_consent_new_dwg_flag
		, property_consent_builders
		-- Please note this logic may have changed due to feedback - this is duplicate logic that embedded in the BL code.
		, CASE    
		    WHEN property_land_smpl_zne_desc = 'Residential' and property_category_desc  <> 'Vacant' then 'Residential'
            WHEN  property_land_smpl_zne_desc = 'Residential' and property_category_desc  = 'Vacant' AND property_land_use_desc like '%Residential%' then 'Residential'
            WHEN  property_land_smpl_zne_desc = 'Residential' and property_category_desc  = 'Vacant' AND property_land_use_desc like '%Lifestyle%' then 'Residential'
            WHEN  property_land_smpl_zne_desc = 'Residential' and property_category_desc  = 'Vacant' AND property_land_use_desc like '%Bach%' then 'Residential'
            WHEN property_land_zone_desc  LIKE '%Residential%' then 'Residential'
            WHEN property_land_zone_desc  LIKE '%Lifestyle%' then 'Residential'
            WHEN property_land_use_desc  IN ('Single Unit - Lifestyle', 'Bach','Residential','Vacant Lifestyle') 
                AND property_category_desc IN('Improved','Bare Block','Vacant**')  then 'Residential'
            ELSE 'Non-Residential'
         END as property_residential_indicator
		-- determines where the title issue date falls into which financial year
		/*##can be removed START##*/
        , case when title_issue_date >= cal_fin_year_start_16_17 AND title_issue_date <= cal_fin_year_end_16_17 then 'Y' else 'N' end as cal_fin_ttl_year_16_17_prpty_flag
        , case when title_issue_date >= cal_fin_year_start_17_18 AND title_issue_date <= cal_fin_year_end_17_18 then 'Y' else 'N' end as cal_fin_ttl_year_17_18_prpty_flag 
        , case when title_issue_date >= cal_fin_year_start_18_19 AND title_issue_date <= cal_fin_year_end_18_19 then 'Y' else 'N' end as cal_fin_ttl_year_18_19_prpty_flag 
        , case when title_issue_date >= cal_fin_year_start_19_20 AND title_issue_date <= cal_fin_year_end_19_20 then 'Y' else 'N' end as cal_fin_ttl_year_19_20_prpty_flag 		
        , case when title_issue_date >= extraction_run_date_minus_12_mnths AND title_issue_date <= extraction_run_date then 'Y' else 'N' end as last_12_mnths_ttl_prpty_flag 
		-- determines where the dwg date falls into which financial year
        , case when property_consent_lat_iss_date >= cal_fin_year_start_16_17 AND property_consent_lat_iss_date <= cal_fin_year_end_16_17 then 'Y' else 'N' end as cal_fin_year_16_17_ndwg_flag
        , case when property_consent_lat_iss_date >= cal_fin_year_start_17_18 AND property_consent_lat_iss_date <= cal_fin_year_end_17_18 then 'Y' else 'N' end as cal_fin_year_17_18_ndwg_flag 
        , case when property_consent_lat_iss_date >= cal_fin_year_start_18_19 AND property_consent_lat_iss_date <= cal_fin_year_end_18_19 then 'Y' else 'N' end as cal_fin_year_18_19_ndwg_flag 
        , case when property_consent_lat_iss_date >= cal_fin_year_start_19_20 AND property_consent_lat_iss_date <= cal_fin_year_end_19_20 then 'Y' else 'N' end as cal_fin_year_19_20_ndwg_flag 		
        , case when property_consent_lat_iss_date >= extraction_run_date_minus_12_mnths AND property_consent_lat_iss_date <= extraction_run_date then 'Y' else 'N' end as last_12_mnths_ndwg_flag 
        /*##can be removed## END*/

	, extraction_run_date
        , extraction_run_date_minus_12_mnths /*##can be removed##*/
            from (
                 select distinct 
                          title_no as title_title_no
                        , issue_date as title_issue_date 
                        , pty.qpid as property_qpid 
                        , pty.building_name  as property_building_name
						, case    when pty.building_age >= 1800 and pty.building_age <=1900 then 'old'
										when pty.building_age > 1900 and pty.building_age <=2000 then 'new'
										when pty.building_age > 2000  then 'very new'
										when pty.building_age < 1800 and pty.building_age >0 then 'very old' else 'unknown' 
						  end as property_build_age_group
						, pty.building_age as property_build_age_act
                        , pty.address  as property_address
                        , pty.suburb  as property_suburb
                        , pty.town  as property_town
                        , pty.postcode as property_postcode
                        , tms.timeshare_week_no as property_timeshare_week_no
                        , pty.category_code as property_category_code
                        , lcl.category_desc as property_category_desc
                        , pty.land_zone_code as property_land_zone_code
                        , lzl.land_zone_desc as property_land_zone_desc
                        , case when lzl.land_zone_desc like 'Rural%' then 'Rural'
                                when lzl.land_zone_desc like 'Commercial%' then 'Commercial'
                                when lzl.land_zone_desc like 'Residential%' then 'Residential'
                                when lzl.land_zone_desc like 'Other%' then 'Other'
                                when lzl.land_zone_desc like 'Recreational%' then 'Recreational'
                                when lzl.land_zone_desc like 'Reserved Land%' then 'Reserved Land'
                                when lzl.land_zone_desc like 'Lifestyle%' then 'Lifestyle'
                                when lzl.land_zone_desc like 'Community Uses%' then 'Community Uses'
								when lzl.land_zone_desc like 'Industrial%' then 'Industrial'
                                else lzl.land_zone_desc end as property_land_smpl_zne_desc
                        , pty.land_use_code as property_land_use_code
						, lul.land_use_desc as property_land_use_desc
                        , pty.building_floor_area  as property_building_floor_area
                        , case when pty.land_area <= 0.0200 then '200 m2 and below'
                            when pty.land_area > 0.0200 and pty.land_area <= 0.0500  then 'between 200m2 and 500m2 hectares'
                            when pty.land_area > 0.0500 and pty.land_area <= 0.0800  then 'between 500m2 and 800m2 hectares'
                            else 'Above 800m2' 
                          end as property_land_area_sml_cat 
                        , case when pty.land_area <= 1.0000 then '1 hectare and below'
                            when pty.land_area > 1.0000 and pty.land_area <= 10.0000  then 'between 1 and 10 hectares'
                            when pty.land_area > 10.0000 and pty.land_area <= 50.0000  then 'between 10 and 50 hectares'
                            else 'Above 50 hectares' 
                          end as property_land_area_lrg_cat
                        , pty.land_area as property_land_area
                        , case when pty.active= '1' then 'Y' when  pty.active= '0' then  'N' else 'X' end  as property_status_code
                        , pty.update_date as property_update_date
						, property_consent.property_consent_lat_iss_date
						, property_consent.property_consent_new_dwg_flag
						, property_consent.property_consent_builders
    					, cast('2016-04-01 00:00:00' as timestamp) as  cal_fin_year_start_16_17 /*##can be removed##*/
                        , cast('2017-03-31 00:00:00' as timestamp) as  cal_fin_year_end_16_17/*##can be removed##*/
    					, cast('2017-04-01 00:00:00' as timestamp) as  cal_fin_year_start_17_18/*##can be removed##*/
                        , cast('2018-03-31 00:00:00' as timestamp) as  cal_fin_year_end_17_18/*##can be removed##*/
                        , cast('2018-04-01 00:00:00' as timestamp) as  cal_fin_year_start_18_19/*##can be removed##*/
                        , cast('2019-03-31 00:00:00' as timestamp) as  cal_fin_year_end_18_19/*##can be removed##*/
                        , cast('2019-04-01 00:00:00' as timestamp) as  cal_fin_year_start_19_20/*##can be removed##*/
                        , cast('2020-03-31 00:00:00' as timestamp) as  cal_fin_year_end_19_20/*##can be removed##*/
                        , trunc(now(), 'dd') as extraction_run_date
                        , trunc(months_sub( now(),12), 'dd')  as extraction_run_date_minus_12_mnths/*##can be removed##*/
                from lab_fcp_raw_restricted_access.pty_l_title ttl
                /* Joining on these tables creates data issues due to many qpids/ addresses for 1 title*/
                 left join lab_fcp_raw_restricted_access.pty_cl_property_match ptym on ttl.title_no= ptym.ct and ptym.match_quality IN(2) -- high quality match only DIPPRP-116 ; there is a chance only a QPID at level 1 or 0 exisits thus ignoring it
                 left join lab_fcp_raw_restricted_access.pty_cl_property pty on pty.qpid=ptym.qpid and pty.active = '1'
                 left join lab_fcp_raw_restricted_access.pty_cl_land_zone_lkp lzl on lzl.land_zone_code = pty.land_zone_code
                 left join lab_fcp_raw_restricted_access.pty_cl_category_lkp lcl on lcl.category_code=pty.category_code
				 left join lab_fcp_raw_restricted_access.pty_cl_land_use_lkp lul on lul.land_use_code=pty.land_use_code
				 left join lab_fcp_raw_restricted_access.pty_l_title_estate tms on tms.ttl_title_no = ttl.title_no
				 
				 /*Join consent flag BEGIN */
				 
				 left join 
				 
				-- query ensures no duplicates returned in producing a simple flag for new dwellings with garages
				-- keep in mind this flag may have some quality issues in that a qpid can have 20 consents and the new development of the dwg 
				-- can be a different consent date to the max returned 
				-- example 2926094 has a different min and max date but flag is =Y this means likely more than 1 consent
				-- select * from cp_pty_cl_building_consent where qpid=2926094
				
				/* 	Example where a title was issued 1919-07-02 00:00:00 and the dwg property had a consent for new property 
					on 2012-01-01 00:00:00
					select * from cp_pty_cl_building_consent where qpid = 1299073
					select * from cp_pty_cl_property where qpid = 1299073
			
					select ttl.title_no, pty.qpid from cp_pty_l_title ttl
						left join cp_pty_cl_property_match ptym on ttl.title_no= ptym.ct
						left join cp_pty_cl_property pty on pty.qpid=ptym.qpid
						where pty.qpid = 1299073
					
					select * from cp_pty_l_title where title_no = 'CB316/78'
				*/
				
				(	-- this query looks for the maximum consent date for the dwg consents and then looks at concatenating the buildings for that consent, 
					-- sometimes there is multiple builders with different consents on the same day so we concat them with a comma
				select    consent_all_info.qpid
						, consent_all_info.property_consent_lat_iss_date
						, consent_all_info.property_consent_new_dwg_flag
						, group_concat( distinct consent_all_info.property_consent_builders, ', ')  as property_consent_builders
				from (

					select                consent_flag.qpid
										, max_issue_date_join.property_consent_lat_iss_date
										, consent_flag.property_consent_builders
										, case when consent_flag.max_consent_dwg_flag =1 then 'Y' else 'N' end as property_consent_new_dwg_flag 

										from (
											select qpid, property_consent_builders
											, MAX(dwg_flag) as max_consent_dwg_flag 
											, MAX(dwg_issue_date) as property_consent_lat_iss_date --more than one implies 1st consent expired
											from (
														select qpid
														, case when lower(description) like '%dwg%' and lower(description) NOT like '%demo%'  
															   then builder_name else NULL end as property_consent_builders
														, case when lower(description) like '%dwg%' and lower(description) NOT like '%demo%'  then issue_date else NULL end as dwg_issue_date
														, case when lower(description) like '%dwg%' and lower(description) NOT like '%demo%'  then 1 else 0 end as dwg_flag
														from lab_fcp_raw_restricted_access.pty_cl_building_consent 
														where lower(consent_type)='new'
														and lower(description) NOT like '%demo%' 
														and lower(description) like '%dwg%'
														and lower(consent_type)='new'-- and qpid = 3004897
											) as aa group by qpid,property_consent_builders
											) as consent_flag 
					
											INNER JOIN  /*latest consent*/
											(
												select qpid
													, MAX(dwg_issue_date) as property_consent_lat_iss_date --more than one implies 1st consent expired
												from (
															select qpid
															, case when lower(description) like '%dwg%' and lower(description) NOT like '%demo%'  then issue_date else NULL end as dwg_issue_date 
															from lab_fcp_raw_restricted_access.pty_cl_building_consent 
															where lower(consent_type)='new'
															and lower(description) NOT like '%demo%' 
															and lower(description) like '%dwg%'
															and lower(consent_type)='new'-- and qpid = 3004897
												) as aa group by qpid
											) as max_issue_date_join
											on max_issue_date_join.qpid=consent_flag.qpid
											and max_issue_date_join.property_consent_lat_iss_date=consent_flag.property_consent_lat_iss_date
											
											) as consent_all_info
											group by 
													  consent_all_info.qpid
													, consent_all_info.property_consent_lat_iss_date
													, consent_all_info.property_consent_new_dwg_flag
				) as property_consent 
						
				on property_consent.qpid = pty.qpid
					/*Join consent flag END */
					
            ) as a 
            ) as b 
			-- use the below for easily filtering out data for the financial years (manually in HUE) based on the TITLE ISSUE DATE ONLY, this doesn't use the min/max dwg dates for consents.
			-- this could be removed if code is embedded in an ETL batch job (all attributes related to this could as well.)
            /*##can be removed## START*/
			WHERE 
                (${cal_fin_ttl_year_16_17_prpty_flag='Y','N'} = 'Y' AND cal_fin_ttl_year_16_17_prpty_flag= 'Y' AND ${all_years_data='Y','N'}='N')
            OR
                (${cal_fin_ttl_year_17_18_prpty_flag='Y','N'} = 'Y' AND cal_fin_ttl_year_17_18_prpty_flag= 'Y' AND ${all_years_data='Y','N'}='N')
			OR
				(${cal_fin_ttl_year_18_19_prpty_flag='Y','N'} = 'Y' AND cal_fin_ttl_year_18_19_prpty_flag= 'Y' AND ${all_years_data='Y','N'}='N')
			OR
				(${cal_fin_ttl_year_19_20_prpty_flag='Y','N'} = 'Y' AND cal_fin_ttl_year_19_20_prpty_flag= 'Y' AND ${all_years_data='Y','N'}='N')
			OR
			    (${all_years_data}='Y') -- this filter ignores the above period specific filters if =Y
			/*##can be removed## END*/
			/*is there a timeshare on the property , then group by these values
			timeshare logic
			*/
		group by  title_title_no
        , title_issue_date
        , property_category_code
        , property_category_desc
        , property_land_zone_code
        , property_land_smpl_zne_desc
        , property_land_zone_desc
        , property_land_use_code
		, property_land_use_desc
        , property_building_floor_area
        , property_land_area
        , property_land_area_sml_cat 
        , property_land_area_lrg_cat
        , property_qpid
        , property_building_name
		, property_build_age_group
		, property_build_age_act
        , property_address
        , property_suburb
        , property_town
        , property_postcode
        , property_update_date
		, property_consent_lat_iss_date
		, property_consent_new_dwg_flag
		, property_consent_builders
		, property_residential_indicator
        , cal_fin_ttl_year_16_17_prpty_flag/*##can be removed##*/
        , cal_fin_ttl_year_17_18_prpty_flag /*##can be removed##*/
        , cal_fin_ttl_year_18_19_prpty_flag /*##can be removed##*/
        , cal_fin_ttl_year_19_20_prpty_flag /*##can be removed##*/
        , last_12_mnths_ttl_prpty_flag 		/*##can be removed##*/
        , cal_fin_year_16_17_ndwg_flag/*##can be removed##*/
        , cal_fin_year_17_18_ndwg_flag /*##can be removed##*/
        , cal_fin_year_18_19_ndwg_flag /*##can be removed##*/
        , cal_fin_year_19_20_ndwg_flag 	/*##can be removed##*/	
        , last_12_mnths_ndwg_flag /*##can be removed##*/
        , extraction_run_date
        , extraction_run_date_minus_12_mnths/*##can be removed##*/
				) AS all_property_dates
            -- resolves the multiple updates in the data for a qpid/title showing the changes over time for the
            -- size and zones etc.., this ensures the latest is selected and minimizes duplicates
            INNER JOIN  /*latest qpid updated record*/
				(   select ttl.title_no, pty.qpid, max(pty.update_date) property_max_update_date 
					from lab_fcp_raw_restricted_access.pty_l_title ttl
					left join lab_fcp_raw_restricted_access.pty_cl_property_match ptym on ttl.title_no= ptym.ct and ptym.match_quality IN(2)
					left join lab_fcp_raw_restricted_access.pty_cl_property pty on pty.qpid=ptym.qpid and pty.active = '1'
					left join lab_fcp_raw_restricted_access.pty_cl_land_zone_lkp lzl on lzl.land_zone_code = pty.land_zone_code /* redundant flagged for removal*/
					left join lab_fcp_raw_restricted_access.pty_cl_category_lkp lcl on lcl.category_code=pty.category_code /* redundant flagged for removal*/
					left join lab_fcp_raw_restricted_access.pty_cl_land_use_lkp lul on lul.land_use_code=pty.land_use_code /* redundant flagged for removal*/
					left join lab_fcp_raw_restricted_access.pty_l_title_estate tms on tms.ttl_title_no = ttl.title_no
					group by ttl.title_no, pty.qpid
				) as max_property_date
			ON max_property_date.property_max_update_date = all_property_dates.property_update_date
			AND max_property_date.qpid=all_property_dates.property_qpid
			AND max_property_date.title_no=all_property_dates.title_title_no
			) as alldata 






--IMPORTANT Please ignore the queries below ------
	 
--AGGREGATE QUERIES BELOW
---year on year for the above query as aggregate totals including the date ranges that the flags are derived from 
--these values can be derived from VIYA dashboard
--2. Number of new properties developed in the last 12 months (based on title issue date, financial year) 
--4. Comparison to the number of properties developed in the previous 12 months

/*
create table lab_property_project.tmp_pty_dip_fin_yoy_ttl_prop
STORED AS PARQUET AS
     
SELECT 
    case    when cal_fin_ttl_year_16_17_prpty_flag='Y' then cast('2016-04-01 00:00:00' as timestamp) end as  cal_fin_ttl_year_start
    ,case   when cal_fin_ttl_year_16_17_prpty_flag='Y' then cast('2017-03-31 00:00:00' as timestamp) end as  cal_fin_ttl_year_end
    , trunc(NOW(),'DD') as extraction_run_date,count(*) total_fin_ttl_year_property
    from tmp_pty_l_cl_new_property
    where cal_fin_ttl_year_16_17_prpty_flag ='Y' 
    group by cal_fin_ttl_year_start, cal_fin_ttl_year_end
UNION
	SELECT 
		case    when cal_fin_ttl_year_17_18_prpty_flag ='Y' then cast('2017-04-01 00:00:00' as timestamp) end as  cal_fin_ttl_year_start_17_18
		,case   when cal_fin_ttl_year_17_18_prpty_flag ='Y' then cast('2018-03-31 00:00:00' as timestamp) end as  cal_fin_ttl_year_end_17_18
		, trunc(NOW(),'DD') as extraction_run_date,count(*) total_fin_year_property
		from   tmp_pty_l_cl_new_property
		where cal_fin_ttl_year_17_18_prpty_flag ='Y' 
		group by cal_fin_ttl_year_start_17_18, cal_fin_ttl_year_end_17_18
UNION
	SELECT     
		case    when cal_fin_ttl_year_18_19_prpty_flag ='Y' then cast('2018-04-01 00:00:00' as timestamp) end as  cal_fin_ttl_year_start_18_19
		,case   When cal_fin_ttl_year_18_19_prpty_flag ='Y' then cast('2019-03-31 00:00:00' as timestamp) end as  cal_fin_ttl_year_end_18_19
		, trunc(NOW(),'DD') as extraction_run_date,count(*) total_fin_year_property
	   from   tmp_pty_l_cl_new_property
	   where cal_fin_ttl_year_18_19_prpty_flag ='Y' 
	   group by cal_fin_ttl_year_start_18_19, cal_fin_ttl_year_end_18_19
UNION
	SELECT     
		case    when cal_fin_ttl_year_19_20_prpty_flag ='Y' then cast('2019-04-01 00:00:00' as timestamp) end as  cal_fin_ttl_year_start_19_20
		,case   when cal_fin_ttl_year_19_20_prpty_flag ='Y' then cast('2020-03-31 00:00:00' as timestamp) end as  cal_fin_ttl_year_end_19_20
		, trunc(NOW(),'DD') as extraction_run_date, count(*) total_fin_ttl_year_property
		from   tmp_pty_l_cl_new_property
		where cal_fin_ttl_year_19_20_prpty_flag ='Y' 
		group by cal_fin_ttl_year_start_19_20, cal_fin_ttl_year_end_19_20
		
		
		*/
--AGGREGATE QUERIES BELOW
-- specific for the new dwg consents

/*
create table lab_property_project.tmp_pty_dip_fin_yoy_ndwg_prop
STORED AS PARQUET AS
     
SELECT 
    case    when cal_fin_year_16_17_ndwg_flag='Y' then cast('2016-04-01 00:00:00' as timestamp) end as  cal_fin_ndwg_year_start
    ,case   when cal_fin_year_16_17_ndwg_flag='Y' then cast('2017-03-31 00:00:00' as timestamp) end as  cal_fin_ndwg_year_end
    , trunc(NOW(),'DD') as extraction_run_date,count(*) total_fin_year_property
    from tmp_pty_l_cl_new_property
    where cal_fin_year_16_17_ndwg_flag ='Y' 
    group by cal_fin_ndwg_year_start, cal_fin_ndwg_year_end
UNION
	SELECT 
		case    when cal_fin_year_17_18_ndwg_flag ='Y' then cast('2017-04-01 00:00:00' as timestamp) end as  cal_fin_ndwg_year_start_17_18
		,case   when cal_fin_year_17_18_ndwg_flag ='Y' then cast('2018-03-31 00:00:00' as timestamp) end as  cal_fin_ndwg_year_end_17_18
		, trunc(NOW(),'DD') as extraction_run_date,count(*) total_fin_year_property
		from   tmp_pty_l_cl_new_property
		where cal_fin_year_17_18_ndwg_flag ='Y' 
		group by cal_fin_ndwg_year_start_17_18, cal_fin_ndwg_year_end_17_18
UNION
	SELECT     
		case    when cal_fin_year_18_19_ndwg_flag ='Y' then cast('2018-04-01 00:00:00' as timestamp) end as  cal_fin_ndwg_year_start_18_19
		,case   When cal_fin_year_18_19_ndwg_flag ='Y' then cast('2019-03-31 00:00:00' as timestamp) end as  cal_fin_ndwg_year_end_18_19
		, trunc(NOW(),'DD') as extraction_run_date,count(*) total_fin_year_property
	   from   tmp_pty_l_cl_new_property
	   where cal_fin_year_18_19_ndwg_flag ='Y' 
	   group by cal_fin_ndwg_year_start_18_19, cal_fin_ndwg_year_end_18_19
UNION
	SELECT     
		case    when cal_fin_year_19_20_ndwg_flag ='Y' then cast('2019-04-01 00:00:00' as timestamp) end as  cal_fin_ndwg_year_start_19_20
		,case   when cal_fin_year_19_20_ndwg_flag ='Y' then cast('2020-03-31 00:00:00' as timestamp) end as  cal_fin_ndwg_year_end_19_20
		, trunc(NOW(),'DD') as extraction_run_date, count(*) total_fin_year_property
		from   tmp_pty_l_cl_new_property
		where cal_fin_year_19_20_ndwg_flag ='Y' 
		group by cal_fin_ndwg_year_start_19_20, cal_fin_ndwg_year_end_19_20
		
		*/
		
/*		
--AGGREGATE QUERIES BELOW		
-- residential properties totals
--1. Number of residential properties in New Zealand (maybe under a certain size to rule out farmland)	
create table lab_property_project.tmp_pty_dip_res_tot
STORED AS PARQUET AS
    select case when npr.property_land_smpl_zne_desc ='Unknown' 
    then npr.property_land_zone_desc else npr.property_land_smpl_zne_desc  end as property_type, npr.property_land_area_sml_cat,npr.property_land_area_lrg_cat
    , trunc(NOW(),'DD') as extraction_run_date, count(*) as property_type_total
    from lab_property_project.tmp_pty_l_cl_new_property  npr
    group by  property_type,  npr.property_land_area_sml_cat,npr.property_land_area_lrg_cat
    order by  count(*) desc
	
	

--AGGREGATE QUERIES BELOW	
--3. Number of Types of new developments – stand-alone housing vs apartments 
create table lab_property_project.tmp_pty_dip_cat_tot
STORED AS PARQUET AS
select npr.property_category_desc , trunc(NOW(),'DD') as extraction_run_date , count(*) as property_category_total
from lab_property_project.tmp_pty_l_cl_new_property  npr
group by npr.property_category_desc
order by count(*) desc


--AGGREGATE QUERIES BELOW
--0.Residential Properties broken down into entity type owning them
--tax statatements and ird number supplied from Linz and CL data
create table lab_property_project.tmp_pty_dip_entity_tot
STORED AS PARQUET AS
select  start_ir_entity_type, property_land_smpl_zne_desc,trunc(NOW(),'DD') as extraction_run_date, count(*) as property_entity_totals from tmp_pty_dip_owners own
inner join tmp_pty_l_cl_new_property pty on pty.title_title_no=own.title_ttl_title_no
group by  start_ir_entity_type, property_land_smpl_zne_desc
order by count(*) desc

*/
