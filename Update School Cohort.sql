INSERT INTO K12INTEL_DW.DTBL_SCHOOL_COHORT_MEMBERS
(SELECT
   school_key
   ,school_code
   ,school_name
   ,'Project Aware' as cohort_name
   ,'Project Aware' as cohort_short_name
   ,' ' as cohort_group
   ,null as cohort_sort
   ,'3619' as district_code
   ,'Manual' as sys_etl_source
   ,sysdate as sys_created
   ,sysdate as sys_updated
   ,'N' as sys_audit_ind
   ,'N' as sys_dummy_ind
   ,0 as sys_partition_value
FROM
    K12INTEL_DW.DTBL_SCHOOLS
WHERE SCHOOL_CODE IN ('29', '52', '71', '38', '18', '432', '69')
 )
 ;
    ;
insert into k12intel_userdata.xtbl_cohort_definitions
values (1015, 'Project Aware', 'Project Aware', 'Schools that are part of the project aware program.', 'Public', 'School', ' ', ' ', ' ', 'Manual', 'Active', 0, '--', 'Dynamic', to_date('07-01-2015','MM-DD-YYYY'), to_date('12-31-9999','MM-DD-YYYY'), null, null, null, null, '3619', 'Y', 'WARDB', sysdate)
; 
select * from k12intel_userdata.xtbl_cohort_membership where cohort_key = 1015
;
INSERT INTO K12INTEL_USERDATA.XTBL_COHORT_MEMBERSHIP
(select
    '1015'
    ,school_code
    ,null
    ,null
    ,school_name
    ,null
    ,null
    ,null
    ,null
    ,null
    ,null
    ,'3619'
    ,'N'
    ,'WARDB'
    ,sysdate
  FROM
    K12INTEL_DW.DTBL_SCHOOLS
  where SCHOOL_CODE IN ('29', '52', '71', '38', '18', '432', '69')
   )
 ;
 commit;
;