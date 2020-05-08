
/*
	Author: 	Mark Openstein
	
	Overview: 	Uses the IRD number as an anchor key for all main datasets between LINZ and START and PD and shows the 
				property ownerships as a per row level not aggregated.

	Known Bugs: 	(a) 	Since the algothim uses the tax statements data and collapses ird ownership data to the top level trustee, there is duplicates
							returned due to having more than once tax statement date for the same title

					(b) 	Multiple trading names retuned in some cases, mostly mitigated users latest version.
			
	Current Verison: Version: 1.3.4

	Date Last Updated: 08/05/2020 

	Version History:
		1.3.4 PD query changes to left outer join to get more results
		1.3.2. added tax_conveyancor column and ownership column calculations for tax_conveyancor
		1.3 updated final merge to add wonership calculationsion based on names and sepertly based on ir numbers.
		1.1-1.2 extract date added back as was lost, fixes to dup precedence
		1.0 final merge 	query adds linz data as precendece since it determins that if tat dataset has an ird number is more correct than PD.
		0.9 moved agg counts one level up and expanded precendce chckes to X and N to remove dups (moved back to X as there was issues with this loigc inclduing more chercks for a single line)
	    0.8: added the owners with no ird  umber for UNION 
	    0.7: added calc_foreign_flag using START data and cahnged verlast to ver=0
		0.6: where clause bug fix in wrong sub select moved to correct select.
	    0.3-0.5: added precedence to handle the multiple values for the citizen/ resident flags  (intorduced by using start names collapsing linz names data)
	    0.2-0.3: Included START names and this resolves trustee heirarchy and removed many colmns creating duplcates of title
	   	0.1-0.2: row_number() partiton updated to include fullname


	####Query Caveats :####

	1. Tax statement data out of date by up to a month (PD title period tables doesn't have this issue because it also uses CoreLogic data).

	2. Using LINZ tax statements data is only from 2014 (PD title period tables doesn't have this issue).

	3. Data doesn't include the owners that have no IRD number specified as this may result in incorrect matching.

	**IMPORTANT variable JOINTYPE have this as INNER JOIN

	Issues:
	
	1. 	duplicates i the LINZ START data regarding two ird numbers for a owner name, the collapsing is not working entirely correctly.
		select * from lab_property_project.tmp_pty_all_owners own where
		own.pd_owner_full_name ='BADGE OVERLOCKERS'
*/

/*################################################### STEP1: CREATE THE LINZ DATA SET TABLE (WITH PRECEDENT TO SET PARTNERSHIPS AND TRUSTS AS NOT HAINVG RESIDENT AND CITIZEN FLAG AVAILABLE -X)*/
/*#########################################################################################################################################################*/

 -- there is name based duplicates with the linz data, but the loigc is based around the ird numbers.so its not really duplicates unless we change logic to the name
    create table lab_property_project.tmp_pty_l_s_owners
    STORED AS PARQUET AS
-- #### (1) top sub-select derives the calculated columns
select 
	 case 
		when  final.calc_total_titles >1 and final.calc_total_titles <=5 then 'Owners with 2-5 titles' 
		when  final.calc_total_titles >5 then 'Owners with more than 5 titles' 
		when  final.calc_total_titles <=1 then 'Owners with 1 title' 
		else 'Unknown' end as calc_all_titles
	, final.start_ir_entity_type
	, final.start_owner_full_name
	, final.tax_ir_entity_type
	, final.tax_ird_number
	, case when final.tax_non_resident_flag ='Y' then 'Non-Resident'
	   when final.tax_non_resident_flag ='N' then 'Resident' else 'Unknown' end as tax_non_resident_flag 
	, case when final.tax_citizen_or_nz_visa_flag = 'Y' then 'Yes'
	   when final.tax_citizen_or_nz_visa_flag = 'N' then 'No' else 'Unknown' end as tax_citizen_or_nz_visa_flag
	, final.tax_transferor_or_transferee
	, final.tax_max_statement_date
	, final.tax_conveyancor
	, final.title_ttl_title_no
	, final.title_maori_land
	, final.title_provisional
	, final.title_register_type
	, final.title_status
	, final.title_type
	, final.title_issue_date
	, NOW() as extraction_run_date
from 
	(
		select 
			last_value (pre_final.calc_record_row_num) OVER 
			(partition by 	pre_final.tax_ird_number -- ownership based on IRD number - there fore the name of the owner can have 30 titles but diffrent ird numbers
			order by 		pre_final.calc_record_row_num 
			range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) as calc_total_titles
			, pre_final.* 
		from 
		( 
			select 
				row_number() OVER 
				(partition by d.tax_ird_number, d.start_owner_full_name 
				order by      d.tax_ird_number, d.start_owner_full_name 
				) as calc_record_row_num, d.*
				from (
				  select
					-- precedence flags step 2
					 	dense_rank() OVER  
					(partition by c.tax_ird_number, c.start_owner_full_name, c.title_ttl_title_no 
					order by      c.tax_ird_number, c.start_owner_full_name, c.title_ttl_title_no , c.calc_prec_citizen_or_nz_visa 
					) as calc_prec_citi_or_nz_visa_num 

					-- precedence flags step 2
					, 	dense_rank() OVER 
					(partition by c.tax_ird_number, c.start_owner_full_name, c.title_ttl_title_no 
					order by      c.tax_ird_number, c.start_owner_full_name, c.title_ttl_title_no , c.calc_prec_tax_non_resident_flag 
					) as calc_prec_tax_non_resi_flag_num 
					, c.* 
			from (
				select  distinct b.*
				from 
					(
						-- #### (2) this select return the last transaction for the IRD number/title, this can be a TTOR or a TTEE, if its TTOR this its not owned
						select 
						    UPPER(NVL(nam.fstrfreeformatname, concat(concat(concat(concat(NVL(fstrfirstname,''),' '),NVL(fstrmiddlename,'')),' '),NVL(fstrlastname,'')))) as start_owner_full_name
							, NVL(cast(txs.ird_number as varchar),'Unknown') as ird_number 
							, ta.ttl_title_no
							, MAX(NVL(txs.statement_date, cast('9000-12-01 00:00:00' as timestamp))) as statement_date
						from cp_pty_l_tax_statement txs  -- not all titles have a tax statement for the owner ='CB18F/1092'
						inner join cp_pty_l_title_action ta on ta.act_tin_id = txs.tin_id 
						inner join  cp_pty_l_title tt on tt.title_no= ta.ttl_title_no
						inner join app_tblid ird on cast(ird.fstrid as bigint)=txs.ird_number 
					    inner join app_tblcustomer cus on cus.flngcustomerkey = ird.flngcustomerkey and ird.record_active_flag = 'Y' and ird.fstridtype= 'IRD' and cus.record_active_flag='Y' 
                        inner join app_tblnamerecord nam on cus.flngcustomerkey= nam.flngcustomerkey and nam.record_active_flag = 'Y' and nam.fintprofilenumber=1 and nam.flngver=0
						left join app_tblnz_customerstd crs on crs.flngdockey=cus.flngdockey and crs.record_active_flag='Y' and nam.fintprofilenumber=1 and nam.flngver=0 --nam.flngverlast=0   -- ADDED THIS 27-11, CHECK!
						group by
							 NVL(cast(txs.ird_number as varchar),'Unknown') 
							, ta.ttl_title_no
							, start_owner_full_name
					) as a

				LEFT JOIN  -- this might need to be an inner join ???? this works because of the limited number of columns stopping duplicates - but inner join may cut out owners

				-- #### (3) this select joins all our columns from the tables to the final select where its not TTOR
				( select  -- the precedence here tries to eliminate records that have duplicates if thee is records that have different statuses e choose the 1st status
					       case when ISNULL(case when txs.ir_entity_type IN ('T','P') then 'X' else  txs.citizen_or_nz_visa end,'X') IN ('X','N') then 1 else 2 end as calc_prec_citizen_or_nz_visa -- precedence flags step 1
					     , case when ISNULL(case when txs.ir_entity_type IN ('T','P') then 'X' else  txs.non_resident_flag end,'X') IN ('X','Y') then 1 else 2 end as calc_prec_tax_non_resident_flag -- precedence flags step 1 
						 , CASE WHEN txs.ir_entity_type = 'I' THEN 'INDVDL'
                           WHEN txs.ir_entity_type = 'C' THEN 'COMPNY'
                           WHEN txs.ir_entity_type = 'T' THEN 'TRUST' 
                           WHEN txs.ir_entity_type = 'P' THEN 'PTNRSP' 
                           WHEN txs.ir_entity_type = 'S' THEN 'SOCITY' 
                           WHEN txs.ir_entity_type = 'M' THEN 'MRIAUT' 
                           WHEN txs.ir_entity_type = 'X' THEN 'TBD' 
                           WHEN txs.ir_entity_type = 'U' THEN 'UNTTST' 
                           WHEN txs.ir_entity_type = 'F' THEN 'SPRFND' 
                           WHEN txs.ir_entity_type = 'A' THEN 'HLDACC'
                           WHEN txs.ir_entity_type = 'D' THEN 'EMBASY'
                           ELSE 'UNKWN' END as start_ir_entity_type
                        ,  UPPER(NVL(nam.fstrfreeformatname, concat(concat(concat(concat(NVL(fstrfirstname,''),' '),NVL(fstrmiddlename,'')),' '),NVL(fstrlastname,'')))) as start_owner_full_name
						, txs.ir_entity_type  as tax_ir_entity_type
						, NVL(cast(txs.ird_number as varchar),'Unknown') as tax_ird_number 
						, txs.transferor_or_transferee as tax_transferor_or_transferee
						, NVL(txs.statement_date, cast('9000-12-01 00:00:00' as timestamp)) as tax_max_statement_date
						, txs.certifier_firm as tax_conveyancor
					--	, txs.live_in_property as tax_live_in_property--*
					    , CASE when txs.ir_entity_type IN ('T','P') then 'X' else  txs.non_resident_flag end as tax_non_resident_flag --*example dup: 100253852
						, CASE when txs.ir_entity_type IN ('T','P') then 'X' else txs.citizen_or_nz_visa  end as tax_citizen_or_nz_visa_flag --*example dup: 100253852
						, ta.ttl_title_no as title_ttl_title_no
						, ISNULL(tt.maori_land,'X')  as title_maori_land
						, ISNULL(tt.provisional,'X') as title_provisional
						, tt.register_type as title_register_type
						, tt.status as title_status
						, tt.type as title_type
						, NVL(tt.issue_date, cast('9000-12-01 00:00:00' as timestamp))  as title_issue_date
					from cp_pty_l_tax_statement txs  -- not all titles have a tax statement for the owner ='CB18F/1092'
					inner join cp_pty_l_title_action ta on ta.act_tin_id = txs.tin_id 
					inner join  cp_pty_l_title tt on tt.title_no= ta.ttl_title_no
					inner join app_tblid ird on cast(ird.fstrid as bigint)=txs.ird_number 
					inner join app_tblcustomer cus on cus.flngcustomerkey = ird.flngcustomerkey and ird.record_active_flag = 'Y' and ird.fstridtype= 'IRD' and cus.record_active_flag='Y' 
                    -- this wil return duplicates becuase we can have multiple active trading names in START. 100002231
                    inner join app_tblnamerecord nam on cus.flngcustomerkey= nam.flngcustomerkey and nam.record_active_flag = 'Y' and nam.fintprofilenumber=1 and nam.flngver=0-- verlast=0 to to select one active trading name, may still be dups fr diffrent names
                    left join app_tblnz_customerstd crs on crs.flngdockey=cus.flngdockey and crs.record_active_flag='Y' and nam.fintprofilenumber=1 and nam.flngver=0 --nam.flngverlast=0

                	) as b
					ON a.statement_date = b.tax_max_statement_date 
					and a.ird_number=b.tax_ird_number -- this can change to join on FULL NAME, but thee is data quality issues if we do this.
					and a.ttl_title_no=b.title_ttl_title_no -- a. title number may need to change to the title column title number not action table
					and b.tax_transferor_or_transferee != 'TTOR'  and tax_ird_number != 'Unknown'-- join on the dates to get TTEE/TTOR value
				) as c 
				) as d
				where calc_prec_citi_or_nz_visa_num=1 AND calc_prec_tax_non_resi_flag_num =1
				order by tax_ird_number, calc_record_row_num -- order by row may not be needed
		) as pre_final 
		order by pre_final.tax_ird_number , pre_final.calc_record_row_num -- order by row may not be needed
	) as final 
-- where  start_owner_full_name='HYE KYUNG PARK'--final.tax_ird_number in ('125764320') and title_ttl_title_no ='16951'
order by final.tax_ird_number,final.calc_record_row_num, final.title_ttl_title_no 

/*################################################### STEP2: CREATE THE PROPERTYDATAMART DATA SET TABLE (WITH PRECEDENT TO SET PARTNERSHIPS AND TRUSTSD AS NOT HAINVG RESIDENT AND CITIZEN FLAG AVAILABLE -X)*/
/*#########################################################################################################################################################*/

--select distinct opr.period_start, opr.period_end,oir.ird_number,opr.owners,tit.title_no--,oir.*,top.*,opr.*,tit.* 

--inner join cp_pty_cl_owner onr on onr.qpid = ttl.qpid and onr.owner_type='Buyer'
	--, ISNULL(regexp_replace(REPLACE(REPLACE(REPLACE(txs.non_resident_flag,"'N'",'Resident'),"'Y'",'Non-Resident'),"'-'",'Unknown' ),'[\(\)\0-9]',''),'Unknown') as tax_non_resident_flag 
	--, ISNULL(regexp_replace(REPLACE(txs.citizen_nzvisa,"'-'",'X'),'[\(\)\0-9]',''), 'Unknown') as  tax_citizen_or_nz_visa_flag

/*############################################ Property DAtamart schema only.##########################################*/
--   create table lab_property_project.tmp_pty_pd_owners
  --  STORED AS PARQUET AS
  
  -- the tax statement ird number is used below, they may or may not provide it we filter out owners with dont have an ird number recorded (dealt with above linz start data)

  -- LAURA NG has an issue whee sdome records provided she is a resident others are not which complicates the overall figures...
--some ird number records coming up so removing them from the data
   create table lab_property_project.tmp_pty_pd_owners
   STORED AS PARQUET AS
select 
	 case 
		when  final.calc_total_titles >1 and final.calc_total_titles <=5 then 'Owners with 2-5 titles' 
		when  final.calc_total_titles >5 then 'Owners with more than 5 titles' 
		when  final.calc_total_titles <=1 then 'Owners with 1 title' 
		else 'Unknown' end as calc_all_titles
	, final.start_ir_entity_type
	, final.pd_owner_full_name
	, final.tax_ir_entity_type
	, final.tax_ird_number
	, case when final.tax_non_resident_flag ='Y' then 'Non-Resident'
			when final.tax_non_resident_flag ='N' then 'Resident' else 'Unknown' end as tax_non_resident_flag 
	, case when final.tax_citizen_or_nz_visa_flag = 'Y' then 'Yes'
			when final.tax_citizen_or_nz_visa_flag = 'N' then 'No'else 'Unknown' end as tax_citizen_or_nz_visa_flag
	, final.tax_transferor_or_transferee
	, final.tax_max_statement_date
	, final.tax_conveyancor
	, final.title_ttl_title_no
	, final.title_maori_land
	, final.title_provisional
	, final.title_register_type
	, final.title_status
	, final.title_type
	, final.title_issue_date
, NOW() as extraction_run_date
from 
	(
		select 
			last_value (pre_final.calc_record_row_num) OVER 
			(partition by 	pre_final.pd_owner_full_name
			order by 		pre_final.calc_record_row_num 
			range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) as calc_total_titles
			, pre_final.* 
		from 
		( 
			select 
					row_number() OVER 
					(partition by c.pd_owner_full_name
					order by      c.pd_owner_full_name
					) as calc_record_row_num
					, c.* 
			from (
                    select 
                    -- precedence flags step 2
					 	dense_rank() OVER  
					(partition by core_data.pd_owner_full_name, core_data.title_ttl_title_no 
					order by      core_data.pd_owner_full_name, core_data.title_ttl_title_no , core_data.calc_prec_citizen_or_nz_visa 
					) as calc_prec_citi_or_nz_visa_num 

					-- precedence flags step 2
					, 	dense_rank() OVER 
					(partition by  core_data.pd_owner_full_name,  core_data.title_ttl_title_no 
					order by       core_data.pd_owner_full_name,  core_data.title_ttl_title_no  , core_data.calc_prec_tax_non_resident_flag 
					) as calc_prec_tax_non_resi_flag_num 
                        , core_data.pd_owner_full_name
                        , core_data.agg_owners
                        , core_data.tax_ir_entity_type
                        , core_data.start_ir_entity_type
                        , isnull(core_data.tax_ird_number,'Unknown')  as tax_ird_number
                        , core_data.tax_non_resident_flag  
                        , core_data.tax_citizen_or_nz_visa_flag  
                        , core_data.tax_transferor_or_transferee
                        , max_txt_date.tax_max_statement_date
                        , core_data.tax_conveyancor
                        , core_data.title_ttl_title_no
                        , core_data.title_maori_land
                        , core_data.title_provisional
                        , core_data.title_register_type
                        , core_data.title_status
                        , core_data.title_type
                        , core_data.title_issue_date
                    from 
                    (
                        select  distinct 
                           case when ISNULL(case when opr.ir_entity_types IN ('T','P')  then 'X' else  txf.citizen_or_nz_visa end,'X') IN ('X','N') then 1 else 2 end as calc_prec_citizen_or_nz_visa -- precedence flags step 1
					     , case when ISNULL(case when opr.ir_entity_types IN ('T','P')  then 'X' else  txf.non_resident_flag end,'X') IN ('X','Y') then 1 else 2 end as calc_prec_tax_non_resident_flag -- precedence flags step 1 
                         , CASE WHEN opr.ir_entity_types = 'I' THEN 'INDVDL'
                                   WHEN opr.ir_entity_types = 'C' THEN 'COMPNY'
                                   WHEN opr.ir_entity_types = 'T' THEN 'TRUST' 
                                   WHEN opr.ir_entity_types = 'P' THEN 'PTNRSP' 
                                   WHEN opr.ir_entity_types = 'S' THEN 'SOCITY' 
                                   WHEN opr.ir_entity_types = 'M' THEN 'MRIAUT' 
                                   WHEN opr.ir_entity_types = 'X' THEN 'TBD' 
                                   WHEN opr.ir_entity_types = 'U' THEN 'UNTTST' 
                                   WHEN opr.ir_entity_types = 'F' THEN 'SPRFND' 
                                   WHEN opr.ir_entity_types = 'A' THEN 'HLDACC'
                                   WHEN opr.ir_entity_types = 'D' THEN 'EMBASY'
                                   ELSE 'UNKWN'
                            END as start_ir_entity_type  
                        	, ISNULL(UPPER(txf.full_name),upper(opr.owners)) as pd_owner_full_name, opr.owners as agg_owners
                        	, isnull(opr.ir_entity_types,'X') as tax_ir_entity_type -- WHEN THERE IS MULTI OWNERS THEN ITS NULL
                        	, cast(txf.ir_ird_number as varchar) as tax_ird_number
							, CASE when opr.ir_entity_types IN ('T','P') then 'X' else  txf.non_resident_flag end as tax_non_resident_flag --*example dup: 100253852
							, CASE when opr.ir_entity_types IN ('T','P') then 'X' else txf.citizen_or_nz_visa  end as tax_citizen_or_nz_visa_flag--*example dup: 100253852
                        	, txf.transferor_or_transferee as tax_transferor_or_transferee
                        	, ISNULL(txf.statement_date, cast('9000-12-01 00:00:00' as timestamp)) as tax_max_statement_date
                        	, txf.certifier_firm as tax_conveyancor
                        	, ttl.title_no as title_ttl_title_no
                        	, ISNULL(ttl.maori_land,'X') as title_maori_land 
                        	, 'X' as title_provisional
                        	, title_reg_type as title_register_type
                        	, ttl.status as title_status
                        	, ttl.title_type as title_type  
                        	, ttl.issue_date as title_issue_date
                                from pty_pd_title_ownership_period opr
                        inner join pty_pd_title ttl on ttl.title_sk = opr.title_sk
                        inner join pty_pd_top_owner top on top.top_sk =opr.top_sk 
                        left join pty_pd_tax_stmt_summary txs on txs.title_sk = ttl.title_sk and txs.transfer_type = 'Transferee' -- dont need tax statement to avoid dups
                        left join pty_pd_tax_stmt txf on txf.instr_id=txs.instr_id and txf.transferor_or_transferee='TTEE' -- we use this table to get the name info as a transpose version to avoid logic complications using the opr owners value (but we dont have all values)
                        where opr.period_end= cast('9999-12-31 00:00:00' as timestamp)   and txf.ir_ird_number IS NULL
                     ) as core_data 
                    ${joinType=left join,inner join} -- inner join
                        ( -- doesn't resolve the precedence issue for tax statements
                            select ttl.title_no as title_ttl_title_no
                            	 ,  MAX(ISNULL(txf.statement_date, cast('9000-12-01 00:00:00' as timestamp)) ) as tax_max_statement_date
                              from pty_pd_title_ownership_period opr
                              inner join pty_pd_title ttl on ttl.title_sk = opr.title_sk
                              inner join pty_pd_top_owner top on top.top_sk =opr.top_sk 
                              --statement dates different in these two tables...
                              left join pty_pd_tax_stmt_summary txs on txs.title_sk = ttl.title_sk and txs.transfer_type = 'Transferee'-- dont need tax statement to avoid dups
                              left join pty_pd_tax_stmt txf on txf.instr_id=txs.instr_id and txf.transferor_or_transferee='TTEE' -- not all records have txs.instr_id  therefore wont have a name recor din this table for us to retrieve 
                            where opr.period_end= cast('9999-12-31 00:00:00' as timestamp) and txf.ir_ird_number IS NULL
                            GROUP BY title_ttl_title_no
                        ) as max_txt_date 
                    ON max_txt_date.tax_max_statement_date = core_data.tax_max_statement_date
                    AND max_txt_date.title_ttl_title_no=core_data.title_ttl_title_no
			/* owners that is not null essentially turns the left joins to tax statemsn above into inner joins..... ####review this.#### */
                    WHERE core_data.pd_owner_full_name is not null /*some owners have the trust there too in PD !in CL */
                    order by pd_owner_full_name
                    ) as c
                    where calc_prec_citi_or_nz_visa_num=1 AND calc_prec_tax_non_resi_flag_num =1
				    order by pd_owner_full_name, calc_record_row_num -- order by row may not be needed
                    ) as pre_final 
                    order by pre_final.pd_owner_full_name , pre_final.calc_record_row_num -- order by row may not be needed
) as final --where pd_owner_full_name ='HYE KYUNG PARK'--and title_ttl_title_no = 'NA83B/471'
order by final.pd_owner_full_name,final.calc_record_row_num, final.title_ttl_title_no 


/* thyere s dups across datasets */

create table lab_property_project.tmp_pty_dip_owners
STORED AS PARQUET AS


/*################################################### STEP3: CREATE THE MERGED DATA SET TABLE (WITH LINZ DATA WITH IRD NUMBER AS PRECEDENT)*/
/*#########################################################################################################################################################*/
-- RESOLVES DUPLIOCATES BETWEEEN THE DATASETS, HOWEVER calc_all_titles IS CALCULATED PER DATASET
-- AND WOULD NEED OT SHIFT TO FINAL QUERY TO CALCULATE POST PRECEDENCE FILTERS (possible bug but might be limited.)
--linz has time in the issue date so we need to remove this.
-- we detect ownership bsed on the names only and we can use based on names with ird number to detect ownerships

select 
	 case 
		when  final.calc_total_titles >1 and final.calc_total_titles <=5 then 'Owners with 2-5 titles' 
		when  final.calc_total_titles >5 then 'Owners with more than 5 titles' 
		when  final.calc_total_titles <=1 then 'Owners with 1 title' 
		else 'Unknown' end as calc_name_ir_titles_final 
		
		,case 
		when  final.calc_total_titles_name >1 and final.calc_total_titles_name <=5 then 'Owners with 2-5 titles' 
		when  final.calc_total_titles_name >5 then 'Owners with more than 5 titles' 
		when  final.calc_total_titles_name <=1 then 'Owners with 1 title' 
		else 'Unknown' end as calc_name_only_titles_final 
		
		,case 
		when  final.calc_total_titles_conveyancor >1 and final.calc_total_titles_conveyancor <=5 then 'Owners with 2-5 titles' 
		when  final.calc_total_titles_conveyancor >5 then 'Owners with more than 5 titles' 
		when  final.calc_total_titles_conveyancor <=1 then 'Owners with 1 title' 
		else 'Unknown' end as calc_conveyancor_titles_final 
	      --, final.calc_total_titles_conveyancor -- shows the record count basd on the grouping
	      --, final.calc_record_row_num_conveyancor --shows the last value in the grouping
              --, final.calc_pre_titles
        	, final.start_ir_entity_type
        	, final.owner_full_name 
        	, final.tax_ir_entity_type
        	, final.tax_ird_number
        	, final.tax_non_resident_flag 
        	, final.tax_citizen_or_nz_visa_flag 
        	, final.tax_transferor_or_transferee
        	, final.tax_max_statement_date
        	, final.tax_conveyancor
        	, final.title_ttl_title_no
        	, final.title_maori_land
        	, final.title_provisional
        	, final.title_register_type
        	, final.title_status
        	, final.title_type
        	, final.title_issue_date
        	, final.extraction_run_date
        	, final.source
from 
	(
    select 
    			last_value (pre_final.calc_record_row_num) OVER 
    			(partition by 	pre_final.owner_full_name, pre_final.tax_ird_number
    			order by 		pre_final.calc_record_row_num 
    			range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) as calc_total_titles
				
    			, last_value (pre_final.calc_record_row_num_name_only) OVER 
    			(partition by 	pre_final.owner_full_name
    			order by 		pre_final.calc_record_row_num_name_only 
    			range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) as calc_total_titles_name
				
				, last_value (pre_final.calc_record_row_num_conveyancor) OVER 
    			(partition by 	pre_final.owner_full_name, pre_final.tax_conveyancor
    			order by 		pre_final.calc_record_row_num_conveyancor 
    			range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) as calc_total_titles_conveyancor
				
    			, pre_final.* 
    from (
        select  -- owners by name and IRD# grouping
                row_number() OVER 
				(partition by pre_stag.tax_ird_number, pre_stag.pd_owner_full_name 
				order by      pre_stag.tax_ird_number, pre_stag.pd_owner_full_name 
				) as calc_record_row_num
				
			,   -- owners by name only grouping   
                row_number() OVER 
				(partition by pre_stag.pd_owner_full_name 
				order by      pre_stag.pd_owner_full_name 
				) as calc_record_row_num_name_only
				
				-- owners by name and tax_conveyancor grouping
			,   row_number() OVER 
				(partition by pre_stag.pd_owner_full_name , pre_stag.tax_conveyancor 
				order by      pre_stag.pd_owner_full_name,  pre_stag.tax_conveyancor
				) as calc_record_row_num_conveyancor
				
            , pre_stag.calc_all_titles as calc_pre_titles
        	, pre_stag.start_ir_entity_type
        	, pre_stag.pd_owner_full_name as owner_full_name
        	, pre_stag.tax_ir_entity_type
        	, pre_stag.tax_ird_number
        	, pre_stag.tax_non_resident_flag 
        	, pre_stag.tax_citizen_or_nz_visa_flag 
        	, pre_stag.tax_transferor_or_transferee
        	, pre_stag.tax_max_statement_date
        	, pre_stag.tax_conveyancor
        	, pre_stag.title_ttl_title_no
        	, pre_stag.title_maori_land
        	, pre_stag.title_provisional
        	, pre_stag.title_register_type
        	, pre_stag.title_status
        	, pre_stag.title_type
        	, pre_stag.title_issue_date
        	, pre_stag.extraction_run_date
        	, pre_stag.source
from (
select 
   CASE WHEN  prec_src_chk.prec_src_chk  LIKE '%LINZSTART%' AND all_data.source ='LINZSTART' THEN 1 
        WHEN  prec_src_chk.prec_src_chk  = 'LINZSTART' AND all_data.source ='LINZSTART' THEN 1 
        WHEN  prec_src_chk.prec_src_chk  = 'PROPERTYDATAMART' AND all_data.source ='PROPERTYDATAMART' THEN 1 
   ELSE 0 END AS prec_src_chk_case
   , prec_src_chk.prec_src_chk , all_data.* 
        from (
        select 
        	  pto.calc_all_titles
        	, pto.start_ir_entity_type
        	, pto.pd_owner_full_name
        	, pto.tax_ir_entity_type
        	, pto.tax_ird_number
        	, pto.tax_non_resident_flag 
        	, pto.tax_citizen_or_nz_visa_flag 
        	, pto.tax_transferor_or_transferee
        	, pto.tax_max_statement_date
			, pto.tax_conveyancor
        	, pto.title_ttl_title_no
        	, pto.title_maori_land
        	, pto.title_provisional
        	, pto.title_register_type
        	, pto.title_status
        	, pto.title_type
        	, pto.title_issue_date
        	, pto.extraction_run_date
        	, 'PROPERTYDATAMART' as source
        from  tmp_pty_pd_owners   pto
        
        UNION
        
        select 
        	  lso.calc_all_titles
        	, lso.start_ir_entity_type
        	, lso.start_owner_full_name
        	, lso.tax_ir_entity_type
        	, lso.tax_ird_number
        	, lso.tax_non_resident_flag 
        	, lso.tax_citizen_or_nz_visa_flag 
        	, lso.tax_transferor_or_transferee
        	, lso.tax_max_statement_date
			, lso.tax_conveyancor
        	, lso.title_ttl_title_no
        	, lso.title_maori_land
        	, lso.title_provisional
        	, lso.title_register_type
        	, lso.title_status
        	, lso.title_type
        	, cast(trunc(lso.title_issue_date,'DD') as timestamp) as title_issue_date -- remove time from linz data as its not in PD
        	, lso.extraction_run_date
        	, 'LINZSTART' as source
        from  tmp_pty_l_s_owners   lso
        ) as all_data  
INNER JOIN 
    (
        select 
                      GROUP_CONCAT(source,'|') as  prec_src_chk
                	, all_data.pd_owner_full_name
                	, all_data.title_ttl_title_no
                	, all_data.title_issue_date
                from (
                select 
                	 pto.pd_owner_full_name
                	, pto.title_ttl_title_no
                	, pto.title_issue_date
                	, 'PROPERTYDATAMART' as source
                from  tmp_pty_pd_owners   pto 
                UNION
                select 
                	 lso.start_owner_full_name
                	, lso.title_ttl_title_no
                	, cast(trunc(lso.title_issue_date,'DD') as timestamp) as title_issue_date -- remove time from issue date 2012-05-24 00:00:00
                	, 'LINZSTART' as source
                from  tmp_pty_l_s_owners   lso
                ) as all_data
                GROUP BY 
                	 all_data.pd_owner_full_name
                	, all_data.title_ttl_title_no
                	, all_data.title_issue_date
    ) as prec_src_chk
ON prec_src_chk.pd_owner_full_name = all_data.pd_owner_full_name
AND prec_src_chk.title_ttl_title_no=all_data.title_ttl_title_no
AND prec_src_chk.title_issue_date= all_data.title_issue_date
) as pre_stag
 WHERE pre_stag.prec_src_chk_case=1
 ) as pre_final
 order by pre_final.owner_full_name , pre_final.calc_record_row_num, pre_final.calc_record_row_num_name_only, pre_final.calc_record_row_num_conveyancor
 ) as final 
 -- where owner_full_name ='HYE KYUNG PARK'--'AARON WAYNE ORCHARD'
  order by final.owner_full_name,final.calc_record_row_num,final.calc_record_row_num_name_only, final.calc_record_row_num_conveyancor, final.title_ttl_title_no 


 