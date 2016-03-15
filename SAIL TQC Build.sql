CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_F_SAR_TQC"
(
    p_PARAM_BUILD_ID            IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.BUILD_NUMBER%TYPE,
    p_PARAM_PACKAGE_ID            IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.PACKAGE_ID%TYPE,
    p_PARAM_TASK_ID                IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.TASK_ID%TYPE,
    p_PARAM_USE_FULL_REFRESH    IN NUMBER,
    p_PARAM_STAGE_SOURCE        IN VARCHAR2,
    p_PARAM_MISC_PARAMS            IN VARCHAR2,
    p_PARAM_EXECUTION_STATUS    OUT NUMBER
) IS
  PRAGMA AUTONOMOUS_TRANSACTION;

    v_SYS_ETL_SOURCE VARCHAR2(50) := 'BUILD_FTBL_SAR_TQC';
    v_WAREHOUSE_KEY NUMBER(10,0) := 0;
    v_AUDIT_BASE_SEVERITY NUMBER(10,0) := 0;
    v_AUDIT_NATURAL_KEY VARCHAR2(512) := '';
    v_BASE_NATURALKEY_TXT VARCHAR(512) := '';
    v_STAT_ROWS_PROCESSED NUMBER(10,0) := 0;
    v_STAT_ROWS_INSERTED NUMBER(10,0) := 0;
    v_STAT_ROWS_UPDATED NUMBER(10,0) := 0;
    v_STAT_ROWS_DELETED NUMBER(10,0) := 0;
    v_STAT_ROWS_EVOLVED NUMBER(10,0) := 0;
    v_STAT_ROWS_AUDITED NUMBER(10,0) := 0;
    v_STAT_DBERR_COUNT NUMBER(10,0) := 0;
/*
 <!--
     Versifit Structured Comment Block
 -->
 <ETL_COMMENTS>
     <AUTHOR>Versifit</AUTHOR>
     <COPYRIGHT>Copyright (c) 2014 VersiFit Technologies LLC. All rights reserved.</COPYRIGHT>
     <DESCRIPTION>
        Procedure    BUILD_FTBL_SAR_TQC
         Remarks      Builds the ftbl_student_at_risk table for Total Quality Credits (TQC)
     </DESCRIPTION>
     <LANGUAGE>PLSQL</LANGUAGE>
     <TYPE>BLD</TYPE>
     <CONNECTOR>EDVANTAGE</CONNECTOR>
     <QUALIFIER></QUALIFIER>
     <TARGETS>
         <TARGET SCHEMA="K12INTEL_DW" NAME="FTBL_STUDENTS_AT_RISK"/>
     </TARGETS>
     <POLICIES>
         <POLICY NAME="REFRESH" APPLICABLE="Y" NOTES=""/>
         <POLICY NAME="NETCHANGE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="DELETE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_BUILD_CONTROL" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_SCHOOL_CONTROL" APPLICABLE="N" NOTES=""/>
     </POLICIES>
     <CHANGE_LOG>
         <CHANGE DATE="12/30/2009" USER="Versifit" VERSION="10.6.0"  DESC="Procedure Created"/>
         <CHANGE DATE="12/01/2010" USER="Josh Meyer" VERSION="10.6.0"  DESC="Changes to DTBL_SCHOOL_DATES/DTBL_SCHOOLS for school_code from number to varchar"/>
         <CHANGE DATE="12/01/2011" USER="Jason Hildebrandt" VERSION="10.6.0"  DESC="Changed SCHOOL_CODE filters to convert to number and compare < 9000 so it did not drop schols 97,99 etc..."/>
         <CHANGE DATE="12/14/2010" USER="Jason Hildebrandt"  VERSION="10.6.0"   DESC="Revised to break TQC into a credit count and GPA calculation"/>
         <CHANGE DATE="10/11/2011" USER="Matt Michala"  VERSION="10.6.0"   DESC="Load 5 new Risk Factors for Period Marks GPA for 4 Core Subjects + 1 Total Subject for High School Grades 9-12 for Current School Year and Semester (RISK_FACTOR_ID is in SLGPAPMKEN,SLGPAPMKMA,SLGPAPMKSC,SLGPAPMKSS,SLGPAPMKTT)"/>                 
         <CHANGE DATE="12/01/2011" USER="Jason Hildebrandt"  VERSION="10.6.0"   DESC="Changed SCHOOL_CODE filters to convert to number and compare < 9000 so it did not drop schols 97,99 etc..."/>
         <CHANGE DATE="12/28/2011" USER="Matt Michala"  VERSION="10.6.0"   DESC="Load 5 new Risk Factors for Period Marks GPA for 4 Core and 1 Total Subjects for MIDDLE SCHOOL Grades 6-8 for the Current School Year and Semester (RISK_FACTOR_ID is in SLGPAPMMEN,SLGPAPMMMA,SLGPAPMMSC,SLGPAPMMSS,SLGPAPMMTT)"/>
     </CHANGE_LOG>
 </ETL_COMMENTS>
 */

    v_start_time                CONSTANT DATE := sysdate;
    v_buffer varchar(30);
    v_const_school_year         CONSTANT k12intel_staging.schools.stage_sis_school_year%type := k12intel_metadata.get_school_year(SYSDATE);
    v_datadate DATE             := get_lastdata_date;

-- Auditing related variables
  v_table_id NUMBER(10);
  v_rowcnt   NUMBER;
BEGIN
  v_table_id := -999;


  /*
    Local variables and cursors
  */
  DECLARE
    v_EXISTING_STUDENT_AT_RISK_KEY NUMBER(10);
    v_begin_grade VARCHAR(2);
    v_end_grade VARCHAR(2);

    TYPE curAtRisk_t IS REF CURSOR;
    cur curAtRisk_t;
    stmt_str VARCHAR2(32767);

    v_ftbl_students_at_risk_record K12INTEL_DW.FTBL_STUDENTS_AT_RISK%ROWTYPE;

    v_student_curent_school_code K12INTEL_DW.DTBL_STUDENTS.STUDENT_CURRENT_SCHOOL_CODE%TYPE;
    v_ftbl_stu_at_risk_rec K12INTEL_DW.FTBL_STUDENTS_AT_RISK%ROWTYPE;

  BEGIN
    DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'pre-cursor');

    -- Obtain the next SCHOOL_KEY when inserting a new record
    --SELECT NVL(MAX(SCHOOL_KEY), 0) + 1 INTO v_SCHOOL_KEY FROM K12INTEL_DW.DTBL_RISK_FACTORS;

   v_ftbl_students_at_risk_record.SYS_ETL_SOURCE   := v_SYS_ETL_SOURCE;
   v_ftbl_students_at_risk_record.SYS_CREATED        := sysdate;
   v_ftbl_students_at_risk_record.SYS_UPDATED        := sysdate;
   v_ftbl_students_at_risk_record.SYS_AUDIT_IND    := 'N';

    BEGIN
        -- create record for current year students for each risk period
        delete from k12intel_staging.STUDENTS_AT_RISK_TEMP
        where RISK_FACTOR_KEY = -1
        ;

        insert into k12intel_staging.STUDENTS_AT_RISK_TEMP
        select dstu.student_key, -1, min_date_value, '--', 0, dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL, null, null, null, mark_period, null, max_date_value, 0, max_date_value - min_date_value, '--'
        from k12intel_dw.dtbl_students dstu
            inner join k12intel_dw.DTBL_STUDENTS_EXTENSION dstu_ext
                on dstu.student_key = dstu_ext.student_key
        inner join (
            select school_code, local_school_year, mark_period, min(date_value) min_date_value, max(date_value) max_date_value
            from (
                        select school_code
                            , date_value
                            , local_school_year,
                            case
                                when date_value < max(case when ROLLING_LOCAL_SEMESTER_NUMBER is null then null else date_value end) over (partition by school_code, LOCAL_SCHOOL_YEAR, mark_period) then
                                    case mark_period when 'MID' then 'BEG' else 'MID' end
                                else mark_period end mark_period
                        from (
                            select dsd.school_code
                                , date_value, LOCAL_SCHOOL_YEAR,
                                ROLLING_LOCAL_SEMESTER_NUMBER,
                                LOCAL_SEMESTER,
                                case when (
                                    max(case when LOCAL_SEMESTER = 0 then 1 else LOCAL_SEMESTER end) over (partition by school_code, local_school_year order by date_value rows between unbounded preceding and current row) /
                                    max(case when LOCAL_SEMESTER = 0 then 1 else LOCAL_SEMESTER end) over (partition by school_code, local_school_year)
                                ) <= .5 then 'MID' else 'END' end mark_period
                            from k12intel_dw.dtbl_school_dates dsd
                            where 1=1
                                and school_code IN (
                                    select distinct student_current_school_code
                                    from k12intel_dw.DTBL_STUDENTS
                                    where STUDENT_CURRENT_GRADE_CODE in ('06','07','08','09','10','11','12')                          -- Matt Michala  12/28/2011   (added grades 6-8)
                                        and student_activity_indicator = 'Active'
                                )
--                                and sis_school_year between get_sis_school_year and get_sis_school_year
                                  and sis_school_year = v_const_school_year 
                        )
             )
             group by school_code, local_school_year, mark_period
             having min(date_value) <= sysdate
         ) periods
            on dstu.STUDENT_CURRENT_SCHOOL_CODE = periods.school_code
         where STUDENT_CURRENT_GRADE_CODE in ('06','07','08','09','10','11','12')                                                   -- Matt Michala  12/28/2011   (added grades 6-8)
            and dstu.STUDENT_ACTIVITY_INDICATOR = 'Active'
        ;

        -- delete the credits and gpa records
        DELETE FROM K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP
        WHERE risk_factor_key IN (
            SELECT risk_factor_key
            FROM k12intel_dw.DTBL_RISK_FACTORS
            WHERE risk_factor_id LIKE 'SLGPA%' OR risk_factor_id LIKE 'SLTCTOT%'
        );

        INSERT INTO K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP
        -- HS Credits                                                                                                                                                      -- Matt Michala  12/28/2011   (added this comment line for clarification)
        select
            marks.student_key,
            risk_factor_key,
            marks.student_risk_identified_date,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 'No' else 'Yes' end credits_on_track,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 0 else 1 end credits_on_track,
            total_credits_earned,
            total_credits_attempted,
            total_credits_earned_tot,
            ct.high_value,
            nvl(ct.RISK_LEVEL, 'Low'),
            NULL,
            marks.risk_factor_effective_end,
            case ct.RISK_LEVEL when 'High' then 2 when 'Moderate' then 1 else 0 end,
            marks.risk_factor_effective_end - marks.student_risk_identified_date,
            drsk.RISK_FACTOR_DEFAULT_ACTION
        from (
            select sart.student_key, sart.STUDENT_RISK_REPORT_TEXT mark_period
                ,max(sart.STUDENT_RISK_IDENTIFIED_DATE) student_risk_identified_date
                ,max(sart.RISK_FACTOR_EFFECTIVE_END) risk_factor_effective_end
                ,sum(case when sub.DATE_VALUE <= sart.STUDENT_RISK_IDENTIFIED_DATE then sub.MARK_CREDIT_VALUE_EARNED else null end) total_credits_earned
                ,sum(case when sub.DATE_VALUE <= sart.STUDENT_RISK_IDENTIFIED_DATE then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) total_credits_attempted
                ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_EARNED else null end) total_credits_earned_tot
            from k12intel_staging.STUDENTS_AT_RISK_TEMP sart
            left join (
                select fmrk.student_key, dsdt.date_value, sum(mark_credit_value_earned) mark_credit_value_earned,  sum(mark_credit_value_attempted) mark_credit_value_attempted
                from
                    K12INTEL_DW.ftbl_final_marks fmrk
                    inner join k12intel_dw.ftbl_diploma_requirements fdpr
                        on fmrk.final_mark_key = fdpr.final_mark_key
                    inner join k12intel_dw.dtbl_school_dates dsdt
                        on fmrk.SCHOOL_DATES_KEY = dsdt.SCHOOL_DATES_KEY
                    inner join k12intel_dw.dtbl_students dstu
                        on fdpr.STUDENT_KEY = dstu.student_key
                    inner join k12intel_dw.dtbl_courses c
                        on fmrk.course_key = c.course_key
                    inner join k12intel_dw.dtbl_scales s
                        on fmrk.scale_key = s.scale_key
                 where 1=1
                   and dstu.STUDENT_CURRENT_GRADE_CODE in ('09','10','11','12')
                    and dstu.STUDENT_ACTIVITY_INDICATOR = 'Active'
                    and fdpr.DIPLOMA_REQUIREMENT_STATUS in ('Attempted','Completed')
                 group by fmrk.student_key, dsdt.date_value
            ) sub
                on sub.student_key = sart.student_key
            where RISK_FACTOR_KEY = -1
                group by sart.student_key, sart.STUDENT_RISK_REPORT_TEXT
        ) marks
        inner join k12intel_dw.DTBL_STUDENTS_EXTENSION dstu_ext
            on marks.student_key = dstu_ext.student_key
                inner join k12intel_dw.DTBL_STUDENTS dstu2                                                                                                                 -- Matt Michala  12/28/2011  (join to DSTU2 to make sure only insert SLCTOTHS records into SART for HS Grades based on the -1 key records)
                on marks.student_key = dstu2.student_key AND  DSTU2.STUDENT_CURRENT_GRADE_CODE in ('09','10','11','12')
        inner join k12intel_dw.dtbl_risk_factors drsk
            on risk_factor_id = 'SLTCTOTHS'
        left join k12intel_userdata.XTBL_SAIL_CREDIT_THRESHOLDS ct
            on ct.years = dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL--case when to_date('07/01' || k12intel_metadata.get_sis_school_year(),'mm/dd/yyyy') > sub.student_risk_identified_date THEN sub.STUDENT_YEARS_IN_HIGH_SCHOOL-1 else sub.STUDENT_YEARS_IN_HIGH_SCHOOL END
                and ct.MARK_PERIOD = marks.mark_period
                and marks.total_credits_earned between low_value and high_value
                and ct.MS_HS = 'H'

        /*
        INSERT INTO K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP
        -- HS Credits
        SELECT
            sub.student_key,
            risk_factor_key,
            sub.student_risk_identified_date,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 'No' else 'Yes' end credits_on_track,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 0 else 1 end credits_on_track,
            total_credits_earned,
            total_credits_attempted,
            NULL,
            ct.high_value,
            nvl(ct.RISK_LEVEL, 'Low'),
            NULL,
            sub.student_risk_identified_date,
            1,
            0,
            drsk.RISK_FACTOR_DEFAULT_ACTION
        from (
                select dstu.student_key, dstu.student_current_school_code, dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL, max(b.date_value) student_risk_identified_date,
                    sum(case when fdr.final_mark_key is null then null else MARK_CREDIT_VALUE_EARNED end) TOTAL_CREDITS_EARNED,
                    sum(case when fdr.final_mark_key is null then null else MARK_CREDIT_VALUE_ATTEMPTED end) TOTAL_CREDITS_ATTEMPTED
                from k12intel_dw.dtbl_students dstu
                    inner join k12intel_dw.ftbl_final_marks ffm
                        on dstu.student_key = ffm.student_key
                    left join k12intel_dw.ftbl_diploma_requirements fdr
                        on ffm.final_mark_key = fdr.final_mark_key
                    inner join k12intel_dw.dtbl_school_dates b
                        on ffm.school_dates_key = b.school_dates_key
                    inner join k12intel_dw.dtbl_courses c
                        on ffm.course_key = c.course_key
                    inner join k12intel_dw.dtbl_scales s
                        on ffm.scale_key = s.scale_key
                    inner join k12intel_dw.DTBL_STUDENTS_EXTENSION dstu_ext
                        on dstu.student_key = dstu_ext.student_key
                    --inner join K12INTEL_DW.DTBL_SCHOOL_DATES_YR_SUMMARY dsdys
                    --    on b.school_code = dsdys.school_code and b.sis_school_year = dsdys.sis_school_year
                where
                    dstu.student_activity_indicator = 'Active' and dstu.student_current_grade_code IN ('09', '10', '11', '12')
                    --and student_current_school_code in (4)
                group by dstu.student_key, dstu.student_current_school_code, dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL
            ) sub
                inner join k12intel_dw.dtbl_risk_factors drsk
                     on risk_factor_id = 'SLTCTOTHS'
            inner join (
                select school_code, date_value,
                    case
                        when date_value < max(case when ROLLING_LOCAL_SEMESTER_NUMBER is null then null else date_value end) over (partition by school_code, LOCAL_SCHOOL_YEAR, mark_period) then
                            case mark_period when 'MID' then 'BEG' else 'MID' end
                        else mark_period end mark_period
                from (
                    select dsd.school_code, date_value, LOCAL_SCHOOL_YEAR,
                        ROLLING_LOCAL_SEMESTER_NUMBER,
                        LOCAL_SEMESTER,
                        --case when (dense_rank() over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR order by LOCAL_TERM) / count(distinct LOCAL_TERM) over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR)) <= .5 then 'MID' else 'END' end mark_period,
                        case when (
                            max(case when LOCAL_SEMESTER = 0 then 1 else LOCAL_SEMESTER end) over (partition by school_code, local_school_year order by date_value rows between unbounded preceding and current row) /
                            max(case when LOCAL_SEMESTER = 0 then 1 else LOCAL_SEMESTER end) over (partition by school_code, local_school_year)
                        ) <= .5 then 'MID' else 'END' end mark_period
                    from k12intel_dw.dtbl_school_dates dsd
                    where 1=1
                        and ROLLING_LOCAL_SCHOOL_YR_NUMBER > -5
                        --and school_code IN (4)
                        --and local_school_year = '2009-2010'
                )
                order by date_value
            ) term
                on sub.student_current_school_code = term.school_code
                    and trunc(sysdate) = term.date_value
            left join k12intel_userdata.XTBL_SAIL_CREDIT_THRESHOLDS ct
                on ct.years = sub.STUDENT_YEARS_IN_HIGH_SCHOOL--case when to_date('07/01' || k12intel_metadata.get_sis_school_year(),'mm/dd/yyyy') > sub.student_risk_identified_date THEN sub.STUDENT_YEARS_IN_HIGH_SCHOOL-1 else sub.STUDENT_YEARS_IN_HIGH_SCHOOL END
                    and ct.MARK_PERIOD = term.mark_period
                    and sub.total_credits_earned between low_value and high_value
                    and ct.MS_HS = 'H'*/
        /*SELECT
            sub.student_key,
            risk_factor_key,
            sub.student_risk_identified_date,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 'No' else 'Yes' end credits_on_track,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 0 else 1 end credits_on_track,
            total_credits_earned,
            total_credits_attempted,
            NULL,
            ct.high_value,
            nvl(ct.RISK_LEVEL, 'Low'),
            NULL,
            sub.student_risk_identified_date,
            1,
            0,
            drsk.RISK_FACTOR_DEFAULT_ACTION
        from (
                select dstu.student_key, dstu.student_current_school_code, dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL, max(dsdys.end_enroll_date) student_risk_identified_date,
                    sum(MARK_CREDIT_VALUE_EARNED) TOTAL_CREDITS_EARNED,
                    sum(MARK_CREDIT_VALUE_ATTEMPTED) TOTAL_CREDITS_ATTEMPTED
                from k12intel_dw.dtbl_students dstu
                    inner join k12intel_dw.ftbl_final_marks ffm
                        on dstu.student_key = ffm.student_key
                    inner join k12intel_dw.ftbl_diploma_requirements fdr
                        on ffm.final_mark_key = fdr.final_mark_key
                    inner join k12intel_dw.dtbl_school_dates b
                        on ffm.school_dates_key = b.school_dates_key
                    inner join k12intel_dw.dtbl_courses c
                        on ffm.course_key = c.course_key
                    inner join k12intel_dw.dtbl_scales s
                        on ffm.scale_key = s.scale_key
                    inner join k12intel_dw.DTBL_STUDENTS_EXTENSION dstu_ext
                        on dstu.student_key = dstu_ext.student_key
                    inner join K12INTEL_DW.DTBL_SCHOOL_DATES_YR_SUMMARY dsdys
                        on b.school_code = dsdys.school_code and b.sis_school_year = dsdys.sis_school_year
                where
                    dstu.student_activity_indicator = 'Active' and dstu.student_current_grade_code IN ('09', '10', '11', '12')
                group by dstu.student_key, dstu.student_current_school_code, dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL
            ) sub
                inner join k12intel_dw.dtbl_risk_factors drsk
                     on risk_factor_id = 'SLTCTOTHS'
            inner join (
                select dsd.school_code, date_value,
                    case when (dense_rank() over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR order by LOCAL_TERM) / count(distinct LOCAL_TERM) over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR)) <= .5 then 'MID' else 'END' end mark_period
                from k12intel_dw.dtbl_school_dates dsd
                where 1=1--school_code IN (14, 4)
                    --and local_school_year = '2010-2011'
                    and LOCAL_TERM is not null --and LOCAL_TERM <> 0
            ) term
                on sub.student_current_school_code = term.school_code
                    and sub.student_risk_identified_date = term.date_value
            left join k12intel_userdata.XTBL_SAIL_CREDIT_THRESHOLDS ct
                on ct.years = case when to_date('07/01' || k12intel_metadata.get_sis_school_year(),'mm/dd/yyyy') > sub.student_risk_identified_date THEN sub.STUDENT_YEARS_IN_HIGH_SCHOOL-1 else sub.STUDENT_YEARS_IN_HIGH_SCHOOL END and ct.MARK_PERIOD = term.mark_period and sub.total_credits_earned between low_value and high_value
                    and ct.MS_HS = 'H'*/

        UNION ALL

-- ****************************************
-- /*  Matt Michala  12/28/2011  -- commented out this entire block of MS Credits from Versifit and replaced with new block below as copied from previous HS Credits select statement above and modified for MS Grades 6-8
--
--        -- MS Credits
--        SELECT
--            sub.student_key,
--            risk_factor_key,
--            sub.student_risk_identified_date,
--            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 'No' else 'Yes' end credits_on_track,
--            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 0 else 1 end credits_on_track,
--            total_credits_earned,
--            total_credits_attempted,
--            NULL,
--            ct.high_value,
--            nvl(ct.RISK_LEVEL, 'Low'),
--            NULL,
--            sub.student_risk_identified_date,
--            1,
--            0,
--            drsk.RISK_FACTOR_DEFAULT_ACTION
--        from (
--                select dstu.student_key, dstu.student_current_school_code, dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL, max(b.date_value) student_risk_identified_date,
--                    sum(MARK_CREDIT_VALUE_EARNED) TOTAL_CREDITS_EARNED,
--                    sum(MARK_CREDIT_VALUE_ATTEMPTED) TOTAL_CREDITS_ATTEMPTED
--                from k12intel_dw.dtbl_students dstu
--                    inner join k12intel_dw.ftbl_final_marks ffm
--                        on dstu.student_key = ffm.student_key
--                    inner join k12intel_dw.dtbl_school_dates b
--                        on ffm.school_dates_key = b.school_dates_key
--                    inner join k12intel_dw.dtbl_courses c
--                        on ffm.course_key = c.course_key
--                    inner join k12intel_dw.dtbl_scales s
--                        on ffm.scale_key = s.scale_key
--                    inner join k12intel_dw.DTBL_STUDENTS_EXTENSION dstu_ext
--                        on dstu.student_key = dstu_ext.student_key
--                    --inner join K12INTEL_DW.DTBL_SCHOOL_DATES_YR_SUMMARY dsdys
--                    --    on b.school_code = dsdys.school_code and b.sis_school_year = dsdys.sis_school_year
--                where
--                    dstu.student_activity_indicator = 'Active' and dstu.student_current_grade_code IN ('06', '07', '08')
--                group by dstu.student_key, dstu.student_current_school_code, dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL
--            ) sub
--                inner join k12intel_dw.dtbl_risk_factors drsk
--                     on risk_factor_id = 'SLTCTOTMS'
--            inner join (
--                /*select dsd.school_code, date_value,
--                    CASE
--                        when (dense_rank() over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR order by LOCAL_TERM) / count(distinct LOCAL_TERM) over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR)) <= .25 then '1ST'
--                        when (dense_rank() over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR order by LOCAL_TERM) / count(distinct LOCAL_TERM) over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR)) <= .5 then '2ND'
--                        when (dense_rank() over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR order by LOCAL_TERM) / count(distinct LOCAL_TERM) over (partition by SCHOOL_CODE, LOCAL_SCHOOL_YEAR)) <= .75 then '3RD'
--                        else 'END'
--                    end mark_period
--                from k12intel_dw.dtbl_school_dates dsd
--                where 1=1--school_code IN (14, 4)
--                    --and local_school_year = '2010-2011'
--                    and LOCAL_TERM is not null --and LOCAL_TERM <> 0*/
--                    select school_code, date_value,
--                        case
--                            when date_value < max(case when ROLLING_LOCAL_TERM_NUMBER is null then null else date_value end) over (partition by school_code, LOCAL_SCHOOL_YEAR, mark_period) then
--                                case mark_period when '1ST' then 'BEG' when '2ND' then '1ST' when '3RD' then '2ND' else '3RD' end
--                            else mark_period end mark_period
--                    from (
--                        select school_code, date_value,
--                            ROLLING_LOCAL_TERM_NUMBER, LOCAL_SCHOOL_YEAR,
--                        CASE when pct <= .25 then '1ST' when pct <= .5 then '2ND' when pct <= .75 then '3RD' else 'END' end mark_period
--                        from (
--                            select dsd.school_code, date_value,
--                                ROLLING_LOCAL_TERM_NUMBER,
--                                LOCAL_SCHOOL_YEAR,
--                                nvl(max(ROLLING_LOCAL_TERM_NUMBER) over (partition by school_code, local_school_year order by date_value rows between unbounded preceding and current row) - min(ROLLING_LOCAL_TERM_NUMBER) over (partition by school_code, local_school_year) + 1, 1) curr_rltn,
--                                (
--                                    nvl(max(ROLLING_LOCAL_TERM_NUMBER) over (partition by school_code, local_school_year order by date_value rows between unbounded preceding and current row) - min(ROLLING_LOCAL_TERM_NUMBER) over (partition by school_code, local_school_year) + 1, 1) /
--                                    nvl(max(ROLLING_LOCAL_TERM_NUMBER) over (partition by school_code, local_school_year) - min(ROLLING_LOCAL_TERM_NUMBER) over (partition by school_code, local_school_year) + 1, 1)
--                                ) pct
--                            from k12intel_dw.dtbl_school_dates dsd
--                            where 1=1
--                                and ROLLING_LOCAL_SCHOOL_YR_NUMBER > -5
--                                --and school_code IN (59)
--                                --and local_school_year = '2010-2011'
--                        )
--                    )
--                    order by date_value
--            ) term
--                on sub.student_current_school_code = term.school_code
--                    and trunc(sysdate) = term.date_value
--            left join k12intel_userdata.XTBL_SAIL_CREDIT_THRESHOLDS ct
--                on ct.years = sub.STUDENT_YEARS_IN_HIGH_SCHOOL--case when to_date('07/01' || k12intel_metadata.get_sis_school_year(),'mm/dd/yyyy') > sub.student_risk_identified_date THEN sub.STUDENT_YEARS_IN_HIGH_SCHOOL-1 else sub.STUDENT_YEARS_IN_HIGH_SCHOOL END
--                    and ct.MARK_PERIOD = term.mark_period
--                    and sub.total_credits_earned between low_value and high_value
--                    and ct.MS_HS = 'M'
--        ;
-- */   END OF MS Credits SELECTION BLOCK COMMENTED OUT BY Matt Michala  02/28/2011
-- ***************************************

        -- **********************************
        -- Matt Michala  02/28/2011   Replaced above MS Credits selection with this new MS Credits select (as modified from HS Credits selection above)
        -- MS Credits
        select
            marks.student_key,
            risk_factor_key,
            marks.student_risk_identified_date,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 'No' else 'Yes' end credits_on_track,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 0 else 1 end credits_on_track,
            total_credits_earned,
            total_credits_attempted,
            total_credits_earned_tot,
            ct.high_value,
            nvl(ct.RISK_LEVEL, 'Low'),
            NULL,
            marks.risk_factor_effective_end,
            case ct.RISK_LEVEL when 'High' then 2 when 'Moderate' then 1 else 0 end,
            marks.risk_factor_effective_end - marks.student_risk_identified_date,
            drsk.RISK_FACTOR_DEFAULT_ACTION
        from (
            select sart.student_key, sart.STUDENT_RISK_REPORT_TEXT mark_period
                ,max(sart.STUDENT_RISK_IDENTIFIED_DATE) student_risk_identified_date
                ,max(sart.RISK_FACTOR_EFFECTIVE_END) risk_factor_effective_end
                ,sum(case when sub.DATE_VALUE <= sart.STUDENT_RISK_IDENTIFIED_DATE then sub.MARK_CREDIT_VALUE_EARNED else null end) total_credits_earned
                ,sum(case when sub.DATE_VALUE <= sart.STUDENT_RISK_IDENTIFIED_DATE then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) total_credits_attempted
                ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_EARNED else null end) total_credits_earned_tot
            from k12intel_staging.STUDENTS_AT_RISK_TEMP sart
            left join (
                select fmrk.student_key, dsdt.date_value, sum(mark_credit_value_earned) mark_credit_value_earned,  sum(mark_credit_value_attempted) mark_credit_value_attempted
                from
                    k12intel_dw.ftbl_final_marks fmrk
                    inner join k12intel_dw.ftbl_diploma_requirements fdpr
                        on fmrk.final_mark_key = fdpr.final_mark_key
                    inner join k12intel_dw.dtbl_school_dates dsdt
                        on fmrk.SCHOOL_DATES_KEY = dsdt.SCHOOL_DATES_KEY
                    inner join k12intel_dw.dtbl_students dstu
                        on fdpr.STUDENT_KEY = dstu.student_key
                    inner join k12intel_dw.dtbl_courses c
                        on fmrk.course_key = c.course_key
                    inner join k12intel_dw.dtbl_scales s
                        on fmrk.scale_key = s.scale_key
                 where 1=1
                   and dstu.STUDENT_CURRENT_GRADE_CODE in ('6','07','08')
                    and dstu.STUDENT_ACTIVITY_INDICATOR = 'Active'
                    and fdpr.DIPLOMA_REQUIREMENT_STATUS in ('Attempted','Completed')
                 group by fmrk.student_key, dsdt.date_value
            ) sub
                on sub.student_key = sart.student_key
            where RISK_FACTOR_KEY = -1
                group by sart.student_key, sart.STUDENT_RISK_REPORT_TEXT
        ) marks
        inner join k12intel_dw.DTBL_STUDENTS_EXTENSION dstu_ext
            on marks.student_key = dstu_ext.student_key
                inner join k12intel_dw.DTBL_STUDENTS dstu2                                                                                                                 -- Matt Michala  12/28/2011  (join to DSTU2 to make sure only insert SLCTOTMS records into SART for MS Grades based on the -1 key records)
                on marks.student_key = dstu2.student_key AND  DSTU2.STUDENT_CURRENT_GRADE_CODE in ('06','07','08')
        inner join k12intel_dw.dtbl_risk_factors drsk
            on risk_factor_id = 'SLTCTOTMS'
        left join k12intel_userdata.XTBL_SAIL_CREDIT_THRESHOLDS ct
            on ct.years = dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL
                and ct.MARK_PERIOD = marks.mark_period
                and marks.total_credits_earned between low_value and high_value
                and ct.MS_HS = 'M'
        -- ****************************************     end of new MS Credits selection block by  Matt Michala  02/28/2011
;

        INSERT INTO K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP
        SELECT
            sub.student_key,
            drsk.RISK_FACTOR_KEY,
            sub.student_risk_identified_date,
            case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 'Yes' else 'No' end credits_on_track,
            case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 1 else 0 end credits_on_track,
            subject_gpa,
            subject_grade_points,
            subject_credits_attempted,
            sub.STUDENT_RISK_MEASURE_VALUE,
            nvl(gt.risk_level, 'Low'),
            case when nvl(halved_grade_points, 0) > 0 then
                cast(halved_grade_points as varchar(10)) || ' grade pts halved for remedial/SPED crs.'
                else null
            end,
            sub.risk_factor_effective_end,
            case gt.RISK_LEVEL when 'High' then 2 when 'Moderate' then 1 else 0 end,
            sub.risk_factor_effective_end - sub.student_risk_identified_date,
            drsk.RISK_FACTOR_DEFAULT_ACTION
            from (
                select sart.student_key, subjects.subject, sart.STUDENT_RISK_IDENTIFIED_DATE
                    ,max(sart.RISK_FACTOR_EFFECTIVE_END) risk_factor_effective_end, max(STUDENT_RISK_MEASURE_VALUE) STUDENT_RISK_MEASURE_VALUE, max(STUDENT_RISK_FACTOR_MET_IND) STUDENT_RISK_FACTOR_MET_IND
                    ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) subject_credits_attempted
                    ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.grade_points else null end) subject_grade_points
                    ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.full_grade_points else null end) full_grade_points
                    ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.halved_grade_points else null end) halved_grade_points
                    ,case when sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) = 0 then 0 else
                        sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.grade_points else null end) /
                        sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end)
                    end subject_gpa
                from k12intel_staging.STUDENTS_AT_RISK_TEMP sart
                    inner join k12intel_dw.DTBL_RISK_FACTORS b
                        on sart.RISK_FACTOR_KEY = b.RISK_FACTOR_KEY
                                and b.RISK_FACTOR_ID = 'SLTCTOTHS'
                    cross join (
                        select DOMAIN_DECODE subject, max(domain_sort) sort
                        from k12intel_userdata.xtbl_domain_decodes
                        where DOMAIN_NAME = 'TQC_SUBJECTS'
                        group by DOMAIN_DECODE
                    ) subjects
                left join (
                    select fmrk.student_key, DOMAIN_DECODE subject, dsdt.date_value, sum(mark_credit_value_earned) mark_credit_value_earned,  sum(mark_credit_value_attempted) mark_credit_value_attempted
                        ,sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED / 2 else MARK_CREDIT_VALUE_EARNED end * s.SCALE_POINT_VALUE) grade_points
                        ,sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED else null end * s.SCALE_POINT_VALUE) halved_grade_points
                        ,sum(MARK_CREDIT_VALUE_EARNED * s.SCALE_POINT_VALUE) full_grade_points
                    from
                        k12intel_dw.ftbl_final_marks fmrk
                        inner join k12intel_dw.ftbl_diploma_requirements fdpr
                            on fmrk.final_mark_key = fdpr.final_mark_key
                        inner join k12intel_dw.dtbl_school_dates dsdt
                            on fmrk.SCHOOL_DATES_KEY = dsdt.SCHOOL_DATES_KEY
                        inner join k12intel_dw.dtbl_students dstu
                            on fdpr.STUDENT_KEY = dstu.student_key
                        inner join k12intel_dw.dtbl_courses c
                            on fmrk.course_key = c.course_key
                        inner join k12intel_dw.dtbl_scales s
                            on fmrk.scale_key = s.scale_key
                        inner join k12intel_userdata.xtbl_domain_decodes xdd
                            on substr(c.COURSE_STATE_EQUIVILENCE_CODE, 1, 2) = xdd.DOMAIN_CODE
                                and xdd.DOMAIN_NAME = 'TQC_SUBJECTS' and xdd.DOMAIN_ALTERNATE_DECODE IN ('HS Core', 'HS Total')
                     where 1=1
                       and dstu.STUDENT_CURRENT_GRADE_CODE in ('09','10','11','12')
                        and dstu.STUDENT_ACTIVITY_INDICATOR = 'Active'
                        and fdpr.DIPLOMA_REQUIREMENT_STATUS in ('Attempted','Completed')
                        and s.scale_abbreviation in ('A','B','C','D','F','U')
                     group by fmrk.student_key, DOMAIN_DECODE, dsdt.date_value
                ) sub
                    on sub.student_key = sart.student_key and sub.subject = subjects.subject
                where 1=1--RISK_FACTOR_KEY = -1
                    --and sart.student_key = 101700
                group by sart.student_key, subjects.subject, sart.STUDENT_RISK_IDENTIFIED_DATE
            ) sub
            left join k12intel_userdata.XTBL_SAIL_GPA_THRESHOLDS gt
               on nvl(subject_gpa, 99) between gt.low_value and gt.HIGH_VALUE
            inner join k12intel_dw.dtbl_risk_factors drsk
                on drsk.risk_factor_id = 'SLGPA' ||
                    case gt.risk_type when 'College' then 'CLG' when 'Graduation' then 'GRD' else 'XXXXXXXXXXXXXX' end ||
                    case sub.subject when 'English' then 'EN' when 'Mathematics' then 'MA' when 'Science' then 'SC' when 'Social Studies' then 'SS' when 'Total' then 'TT' else 'XXXXXXXXX' end
        /*
        INSERT INTO K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP
        -- HS GPA
        SELECT
            sub.student_key,
            drsk.RISK_FACTOR_KEY,
            sub.student_risk_identified_date,
            case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 'Yes' else 'No' end,
            case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 1 else 0 end,
            subject_gpa,STUDENT_KEY
            subject_grade_points,
            subject_credits_attempted,
            sub.STUDENT_RISK_MEASURE_VALUE,
            nvl(gt.risk_level, 'Low'),
            null,
            sub.student_risk_identified_date,
            1,
            0,
            drsk.RISK_FACTOR_DEFAULT_ACTION
            from (
                    select
                        a.student_key, a.student_risk_identified_date, subjects.subject, a.STUDENT_RISK_FACTOR_MET_IND, a.STUDENT_RISK_MEASURE_VALUE
                        --,sum(grade_points) grade_points
                        ,sum(MARK_CREDIT_VALUE_EARNED * s.SCALE_POINT_VALUE) subject_grade_points
                        ,sum(MARK_CREDIT_VALUE_ATTEMPTED) subject_credits_attempted
                        ,case
                            when sum(MARK_CREDIT_VALUE_ATTEMPTED) = 0 then null
                            else trunc(
                                        sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED / 2 else MARK_CREDIT_VALUE_EARNED end * s.SCALE_POINT_VALUE) /
                                        --sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_ATTEMPTED / 2 else MARK_CREDIT_VALUE_ATTEMPTED end)
                                        sum(MARK_CREDIT_VALUE_ATTEMPTED)
                                    , 2
                                )
                        end subject_gpa
                    from K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP a
                        inner join k12intel_dw.DTBL_RISK_FACTORS b
                            on a.RISK_FACTOR_KEY = b.RISK_FACTOR_KEY
                                and b.RISK_FACTOR_ID = 'SLTCTOTHS'
                        cross join (
                            select DOMAIN_DECODE subject, max(domain_sort) sort
                            from k12intel_userdata.xtbl_domain_decodes
                            where DOMAIN_NAME = 'TQC_SUBJECTS'
                            group by DOMAIN_DECODE
                        ) subjects
                    left join (
                        k12intel_dw.ftbl_final_marks ffm
                        inner join k12intel_dw.dtbl_school_dates b
                            on ffm.school_dates_key = b.school_dates_key
                        inner join k12intel_dw.dtbl_courses c
                            on ffm.course_key = c.course_key
                        inner join k12intel_dw.dtbl_scales s
                            on ffm.scale_key = s.scale_key
                        inner join k12intel_userdata.xtbl_domain_decodes xdd
                            on substr(c.COURSE_STATE_EQUIVILENCE_CODE, 1, 2) = xdd.DOMAIN_CODE
                                and xdd.DOMAIN_NAME = 'TQC_SUBJECTS' and xdd.DOMAIN_ALTERNATE_DECODE IN ('HS Core', 'HS Total')
                    )
                        on a.STUDENT_KEY = ffm.STUDENT_KEY
                            and subjects.subject = xdd.DOMAIN_DECODE
                    group by a.student_key, a.student_risk_identified_date, subjects.subject, a.STUDENT_RISK_FACTOR_MET_IND, a.STUDENT_RISK_MEASURE_VALUE
                ) sub
            left join k12intel_userdata.XTBL_SAIL_GPA_THRESHOLDS gt
               on nvl(subject_gpa, 99) between gt.low_value and gt.HIGH_VALUE
            inner join k12intel_dw.dtbl_risk_factors drsk
                on drsk.risk_factor_id = 'SLGPA' ||
                    case gt.risk_type when 'College' then 'CLG' when 'Graduation' then 'GRD' else 'XXXXXXXXXXXXXX' end ||
                    case sub.subject when 'English' then 'EN' when 'Mathematics' then 'MA' when 'Science' then 'SC' when 'Social Studies' then 'SS' when 'Total' then 'TT' else 'XXXXXXXXX' end
        */

        UNION ALL

        -- MS GPA
        SELECT
            sub.student_key,
            drsk.RISK_FACTOR_KEY,
            sub.student_risk_identified_date,
            case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 'Yes' else 'No' end,
            case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 1 else 0 end,
            subject_gpa,
            subject_grade_points,
            subject_credits_attempted,
            sub.STUDENT_RISK_MEASURE_VALUE,
            nvl(gt.risk_level, 'Low'),
            null,
            sub.student_risk_identified_date,
            1,
            0,
            drsk.RISK_FACTOR_DEFAULT_ACTION
            from (
                    select
                        a.student_key, a.student_risk_identified_date, subjects.subject, a.STUDENT_RISK_FACTOR_MET_IND, a.STUDENT_RISK_MEASURE_VALUE
                        --,sum(grade_points) grade_points
                        ,sum(MARK_CREDIT_VALUE_EARNED * s.SCALE_POINT_VALUE) subject_grade_points
                        ,sum(MARK_CREDIT_VALUE_ATTEMPTED) subject_credits_attempted
                        ,case
                            when sum(MARK_CREDIT_VALUE_ATTEMPTED) = 0 then null
                            else trunc(
                                        sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED / 2 else MARK_CREDIT_VALUE_EARNED end * s.SCALE_POINT_VALUE) /
                                        --sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_ATTEMPTED / 2 else MARK_CREDIT_VALUE_ATTEMPTED end)
                                        sum(MARK_CREDIT_VALUE_ATTEMPTED)
                                    , 2
                                )
                        end subject_gpa
                    from K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP a
                        inner join k12intel_dw.DTBL_RISK_FACTORS b
                            on a.RISK_FACTOR_KEY = b.RISK_FACTOR_KEY
                                and b.RISK_FACTOR_ID = 'SLTCTOTMS'
                        cross join (
                            select DOMAIN_DECODE subject, max(domain_sort) sort
                            from k12intel_userdata.xtbl_domain_decodes
                            where DOMAIN_NAME = 'TQC_SUBJECTS'
                            group by DOMAIN_DECODE
                        ) subjects
                    left join (
                        k12intel_dw.ftbl_final_marks ffm
                        inner join k12intel_dw.dtbl_school_dates b
                            on ffm.school_dates_key = b.school_dates_key
                        inner join k12intel_dw.dtbl_courses c
                            on ffm.course_key = c.course_key
                        inner join k12intel_dw.dtbl_scales s
                            on ffm.scale_key = s.scale_key
                        inner join k12intel_userdata.xtbl_domain_decodes xdd
                            on substr(c.COURSE_STATE_EQUIVILENCE_CODE, 1, 2) = xdd.DOMAIN_CODE
                                and xdd.DOMAIN_NAME = 'TQC_SUBJECTS' and xdd.DOMAIN_ALTERNATE_DECODE IN ('MS Core', 'MS Total')
                    )
                        on a.STUDENT_KEY = ffm.STUDENT_KEY
                            and subjects.subject = xdd.DOMAIN_DECODE
                    group by a.student_key, a.student_risk_identified_date, subjects.subject, a.STUDENT_RISK_FACTOR_MET_IND, a.STUDENT_RISK_MEASURE_VALUE
                ) sub
            left join k12intel_userdata.XTBL_SAIL_GPA_THRESHOLDS gt
               on nvl(subject_gpa, 99) between gt.low_value and gt.HIGH_VALUE
            inner join k12intel_dw.dtbl_risk_factors drsk
                on drsk.risk_factor_id = 'SLGPA' ||
                    case gt.risk_type when 'College' then 'CLG' when 'Graduation' then 'GRD' else 'XXXXXXXXXXXXXX' end ||
                    case sub.subject when 'English' then 'EN' when 'Mathematics' then 'MA' when 'Science' then 'SC' when 'Social Studies' then 'SS' when 'Total' then 'TT' else 'XXXXXXXXX' end
        ;

        COMMIT;

           -- ************************************************************************************************************************
           -- Matt Michala   10/11/2011  New Insert Statement for Loading 5 new Risk Factors for Period Marks GPA for 4 Core Subjects + the 1 Total for High School Grades 9-12 for Current School Year and Semester
        INSERT INTO K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP
          SELECT
              sub.student_key,
              drsk.RISK_FACTOR_KEY,
              sub.student_risk_identified_date,
              case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 'Yes' else 'No' end credits_on_track,
              case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 1 else 0 end credits_on_track,
              subject_gpa,
              subject_grade_points,
              subject_credits_attempted,
              sub.STUDENT_RISK_MEASURE_VALUE,
              nvl(gt.risk_level, 'Low'),
              case when nvl(halved_grade_points, 0) > 0 then
                  cast(halved_grade_points as varchar(10)) || ' grade pts halved for remedial/SPED crs.'
                  else null
              end,
              sub.risk_factor_effective_end,
              case gt.RISK_LEVEL when 'High' then 2 when 'Moderate' then 1 else 0 end,
              sub.risk_factor_effective_end - sub.student_risk_identified_date,
              drsk.RISK_FACTOR_DEFAULT_ACTION
              from
                  (
                  select sart.student_key, subjects.subject, sart.STUDENT_RISK_IDENTIFIED_DATE
                      ,max(sart.RISK_FACTOR_EFFECTIVE_END) risk_factor_effective_end, max(STUDENT_RISK_MEASURE_VALUE) STUDENT_RISK_MEASURE_VALUE, max(STUDENT_RISK_FACTOR_MET_IND) STUDENT_RISK_FACTOR_MET_IND
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) subject_credits_attempted
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.grade_points else null end) subject_grade_points
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.full_grade_points else null end) full_grade_points
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.halved_grade_points else null end) halved_grade_points
                      ,case when sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) = 0 then 0 else
                          sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.grade_points else null end) /
                          sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end)
                      end subject_gpa
                  from k12intel_staging.STUDENTS_AT_RISK_TEMP sart

                      inner join k12intel_dw.DTBL_students dss on sart.student_key = dss.student_key                       -- and dss.student_key = 194692     -- FOR TESTING

                      inner join k12intel_dw.DTBL_RISK_FACTORS b
                          on sart.RISK_FACTOR_KEY = b.RISK_FACTOR_KEY
                                  and b.RISK_FACTOR_ID = 'SLTCTOTHS'
                      cross join (
                          select DOMAIN_DECODE subject, max(domain_sort) sort
                          from k12intel_userdata.xtbl_domain_decodes
                          where DOMAIN_NAME = 'TQC_SUBJECTS'
                          group by DOMAIN_DECODE
                      ) subjects
                  left join (
                      select pmrk.student_key, xdd.DOMAIN_DECODE subject, cd.date_value, sum(mark_credit_value_earned) mark_credit_value_earned,  sum(mark_credit_value_attempted) mark_credit_value_attempted
                          ,sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED / 2 else MARK_CREDIT_VALUE_EARNED end * s.SCALE_POINT_VALUE) grade_points
                          ,sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED else null end * s.SCALE_POINT_VALUE) halved_grade_points
                          ,sum(MARK_CREDIT_VALUE_EARNED * s.SCALE_POINT_VALUE) full_grade_points
                      from
                          k12intel_dw.ftbl_period_marks pmrk
                                  inner join  k12intel_dw.dtbl_calendar_dates cd
                                       on pmrk.calendar_date_key = cd.calendar_date_key
                                  inner join k12intel_dw.dtbl_courses c
                                      on pmrk.course_key = c.course_key
                                  inner join k12intel_dw.dtbl_scales s
                                      on pmrk.scale_key = s.scale_key
                                              and s.scale_abbreviation in ('A','B','C','D','F','U')
                                  inner join k12intel_userdata.xtbl_domain_decodes xdd
                                      on substr(c.COURSE_STATE_EQUIVILENCE_CODE, 1, 2) = xdd.DOMAIN_CODE
                                          and xdd.DOMAIN_NAME = 'TQC_SUBJECTS' and xdd.DOMAIN_ALTERNATE_DECODE IN ('HS Core', 'HS Total')
                                  inner join  K12INTEL_DW.MPS_MV_PERIOD_MARKS_LAST_DATE pmdt
                                      on pmrk.student_key = pmdt.student_key
                                          and pmrk.school_key = pmdt.school_key
                                          and pmrk.course_key = pmdt.course_key
                       where
                                  pmdt.max_mark_date = cd.date_value
                                  and pmdt.subject = xdd.domain_decode
                       group by pmrk.student_key, xdd.DOMAIN_DECODE, cd.date_value
                  ) sub
                      on sub.student_key = sart.student_key and sub.subject = subjects.subject
                  group by sart.student_key, subjects.subject, sart.STUDENT_RISK_IDENTIFIED_DATE
              ) sub
              left join k12intel_userdata.XTBL_SAIL_GPA_THRESHOLDS gt
                 on nvl(subject_gpa, 99) between gt.low_value and gt.HIGH_VALUE
                 and gt.risk_type = 'Graduation'
              inner join k12intel_dw.dtbl_risk_factors drsk
                  on drsk.risk_factor_id = 'SLGPAPMK' ||
                      case sub.subject when 'English' then 'EN' when 'Mathematics' then 'MA' when 'Science' then 'SC' when 'Social Studies' then 'SS' when 'Total' then 'TT' else 'XXXXXXXXX' end;
         commit;
           -- end of Matt Michala   10/11/2011
           -- ************************************************************************************************************************

           -- ************************************************************************************************************************
           -- Matt Michala   12/28/2011  New Insert Statement for Loading 5 new Risk Factors for Period Marks GPA for 4 Core Subjects + the 1 Total for MIDDLE SCHOOL Grades 6-8 for Current School Year and Semester
        INSERT INTO K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP
          SELECT
              sub.student_key,
              drsk.RISK_FACTOR_KEY,
              sub.student_risk_identified_date,
              case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 'Yes' else 'No' end credits_on_track,
              case when sub.STUDENT_RISK_FACTOR_MET_IND = 'Yes' or nvl(gt.risk_level, 'Low') <> 'Low' then 1 else 0 end credits_on_track,
              subject_gpa,
              subject_grade_points,
              subject_credits_attempted,
              sub.STUDENT_RISK_MEASURE_VALUE,
              nvl(gt.risk_level, 'Low'),
              case when nvl(halved_grade_points, 0) > 0 then
                  cast(halved_grade_points as varchar(10)) || ' grade pts halved for remedial/SPED crs.'
                  else null
              end,
              sub.risk_factor_effective_end,
              case gt.RISK_LEVEL when 'High' then 2 when 'Moderate' then 1 else 0 end,
              sub.risk_factor_effective_end - sub.student_risk_identified_date,
              drsk.RISK_FACTOR_DEFAULT_ACTION
              from
                  (
                  select sart.student_key, subjects.subject, sart.STUDENT_RISK_IDENTIFIED_DATE
                      ,max(sart.RISK_FACTOR_EFFECTIVE_END) risk_factor_effective_end, max(STUDENT_RISK_MEASURE_VALUE) STUDENT_RISK_MEASURE_VALUE, max(STUDENT_RISK_FACTOR_MET_IND) STUDENT_RISK_FACTOR_MET_IND
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) subject_credits_attempted
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.grade_points else null end) subject_grade_points
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.full_grade_points else null end) full_grade_points
                      ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.halved_grade_points else null end) halved_grade_points
                      ,case when sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) = 0 then 0 else
                          sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.grade_points else null end) /
                          sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end)
                      end subject_gpa
                  from k12intel_staging.STUDENTS_AT_RISK_TEMP sart

                      inner join k12intel_dw.DTBL_students dss on sart.student_key = dss.student_key                       -- and dss.student_key = 191768     -- 6th grader FOR TESTING

                      inner join k12intel_dw.DTBL_RISK_FACTORS b
                          on sart.RISK_FACTOR_KEY = b.RISK_FACTOR_KEY
                                  and b.RISK_FACTOR_ID = 'SLTCTOTMS'
                      cross join (
                          select DOMAIN_DECODE subject, max(domain_sort) sort
                          from k12intel_userdata.xtbl_domain_decodes
                          where DOMAIN_NAME = 'TQC_SUBJECTS'
                          group by DOMAIN_DECODE
                      ) subjects
                  left join
                     (select pmrk.student_key, xdd.DOMAIN_DECODE subject, dsdt.date_value, sum(mark_credit_value_earned) mark_credit_value_earned,  sum(mark_credit_value_attempted) mark_credit_value_attempted
                          ,sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED / 2 else MARK_CREDIT_VALUE_EARNED end * s.SCALE_POINT_VALUE) grade_points
                          ,sum(case when substr(c.COURSE_STATE_EQUIVILENCE_CODE, 6, 1) = 'B' then MARK_CREDIT_VALUE_EARNED else null end * s.SCALE_POINT_VALUE) halved_grade_points
                          ,sum(MARK_CREDIT_VALUE_EARNED * s.SCALE_POINT_VALUE) full_grade_points
                      from
                          k12intel_dw.ftbl_period_marks pmrk
                                inner join k12intel_dw.dtbl_school_dates dsdt
                                    on pmrk.SCHOOL_DATES_KEY = dsdt.SCHOOL_DATES_KEY
                                inner join k12intel_dw.dtbl_students dstu
                                    on pmrk.STUDENT_KEY = dstu.student_key
                                  inner join k12intel_dw.dtbl_courses c
                                      on pmrk.course_key = c.course_key
                                  inner join k12intel_dw.dtbl_scales s
                                      on pmrk.scale_key = s.scale_key
                                              and s.scale_abbreviation in ('A','B','C','D','F','U')
                                  inner join k12intel_userdata.xtbl_domain_decodes xdd
                                      on substr(c.COURSE_STATE_EQUIVILENCE_CODE, 1, 2) = xdd.DOMAIN_CODE
                                          and xdd.DOMAIN_NAME = 'TQC_SUBJECTS' and xdd.DOMAIN_ALTERNATE_DECODE IN ('MS Core', 'MS Total')
                     where 1=1
                        and dstu.STUDENT_CURRENT_GRADE_CODE in ('06','07','08')
                        and dstu.STUDENT_ACTIVITY_INDICATOR = 'Active'
                        and s.scale_abbreviation in ('A','B','C','D','F','U')
                        and dsdt.ROLLING_LOCAL_SCHOOL_YR_NUMBER = 0       -- only include current school year     -- for dwprod use 0, for last semester of  prior school year use -1  (ie for dwdev)
                        and dsdt.ROLLING_LOCAL_SEMESTER_NUMBER  =  0         -- only include current semester            -- for dwprod use 0, for last semester of  prior school year use -1  (ie for dwdev)
                        and pmrk.mark_type = 'Reporting Period'                                -- exclude 'Interim Period' marks
                     group by pmrk.student_key, xdd.DOMAIN_DECODE, dsdt.date_value
                ) sub
                   on sub.student_key = sart.student_key and sub.subject = subjects.subject
                   group by sart.student_key, subjects.subject, sart.STUDENT_RISK_IDENTIFIED_DATE
              ) sub
              left join k12intel_userdata.XTBL_SAIL_GPA_THRESHOLDS gt
                 on nvl(subject_gpa, 99) between gt.low_value and gt.HIGH_VALUE
                 and gt.risk_type = 'Graduation'
              inner join k12intel_dw.dtbl_risk_factors drsk
                  on drsk.risk_factor_id = 'SLGPAPMM' ||
                      case sub.subject when 'English' then 'EN' when 'Mathematics' then 'MA' when 'Science' then 'SC' when 'Social Studies' then 'SS' when 'Total' then 'TT' else 'XXXXXXXXX' end;
         commit;
           -- end of Matt Michala   12/28/2011
           -- ************************************************************************************************************************

    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;

        v_WAREHOUSE_KEY := 0;
        v_AUDIT_BASE_SEVERITY := 0;
        v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

        K12INTEL_METADATA.WRITE_AUDIT(
                p_PARAM_BUILD_ID,
                p_PARAM_PACKAGE_ID,
                p_PARAM_TASK_ID,
                v_SYS_ETL_SOURCE,
                'INSERT CREDIT AND GPA RECORDS',
                v_WAREHOUSE_KEY,
                v_AUDIT_NATURAL_KEY,
                'Untrapped error.',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
        );
    END;

    FOR v_ftbl_stu_at_risk_rec IN (
        SELECT
            NULL STUDENT_AT_RISK_KEY ,a.RISK_FACTOR_KEY ,a.STUDENT_KEY ,NULL STUDENT_EVOLVE_KEY ,NULL SCHOOL_KEY ,NULL SCHOOL_ANNUAL_ATTRIBS_KEY
            ,NULL CALENDAR_DATE_KEY ,NULL SCHOOL_DATES_KEY , STUDENT_RISK_IDENTIFIED_DATE ,NULL STUDENT_RISK_EXPIRE_DATE ,NULL STUDENT_RISK_STATUS
            ,NULL STUDENT_RISK_OUTCOME ,NULL STUDENT_RISK_SEVERITY_SCORE ,NULL STUDENT_RISK_DURATION, a.STUDENT_RISK_FACTOR_MET_IND, a.STUDENT_RISK_FACTOR_MET_VALUE
            ,a.STUDENT_RISK_MEASURE_VALUE ,a.STUDENT_RISK_MEASURE_VALUE_2 ,a.STUDENT_RISK_MEASURE_VALUE_3 ,a.STUDENT_RISK_MEASURE_VALUE_4
            ,a.STUDENT_RISK_REPORT_TEXT ,a.STUDENT_RISK_REPORT_TEXT_2 ,NULL STUDENT_RISK_NOTES ,NULL DISTRICT_CODE ,NULL SYS_ETL_SOURCE
            ,NULL SYS_AUDIT_IND
            ,a.RISK_FACTOR_EFFECTIVE_END
            ,a.RISK_FACTOR_SEVERITY
            ,a.RISK_FACTOR_DEFAULT_DURATION
            ,a.RISK_FACTOR_DEFAULT_ACTION
       FROM K12INTEL_STAGING.STUDENTS_AT_RISK_TEMP a
           INNER JOIN k12intel_dw.dtbl_risk_factors b
                ON a.risk_factor_key = b.risk_factor_key
       WHERE risk_factor_id LIKE 'SLGPA%' OR risk_factor_id LIKE 'SLTCTOT%'
       ORDER BY STUDENT_RISK_IDENTIFIED_DATE
    ) LOOP
    BEGIN
        IF v_ftbl_stu_at_risk_rec.STUDENT_AT_RISK_KEY IS NULL THEN
            -- SYS_AUDIT_IND
            v_ftbl_students_at_risk_record.SYS_AUDIT_IND   := 'N';
            v_ftbl_students_at_risk_record.SYS_PARTITION_VALUE   := 0;
            v_BASE_NATURALKEY_TXT := '@TODO';--'RISK_FACTOR_ID=' || v_risk_factors_data.RISK_FACTOR_ID;

            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'facilities_key');
             -- Obtain the FACILITIES_KEY from the dtbl_facilities table

            ---------------------------------------------------------------
            -- STUDENT_AT_RISK_KEY
            ---------------------------------------------------------------
            BEGIN
                SELECT nvl(max(STUDENT_AT_RISK_KEY), 1000) + 1
                INTO v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY
                FROM k12intel_dw.FTBL_STUDENTS_AT_RISK;

                --v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY := v_students_at_risk_data.RISK_FACTOR_KEY;
                K12INTEL_METADATA.GEN_STUDENT_AT_RISK_KEY
                (
                  v_ftbl_stu_at_risk_rec.STUDENT_KEY,
                  v_ftbl_stu_at_risk_rec.RISK_FACTOR_KEY,
                  v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY
                );

                --v_WAREHOUSE_KEY := v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY;
            EXCEPTION
            WHEN OTHERS THEN
                v_ftbl_stu_at_risk_rec.SYS_AUDIT_IND := 'Y';
                v_ftbl_stu_at_risk_rec.STUDENT_AT_RISK_KEY := -1;
                v_WAREHOUSE_KEY := NULL;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- RISK_FACTOR_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'RISK_FACTOR_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.RISK_FACTOR_KEY := v_ftbl_stu_at_risk_rec.RISK_FACTOR_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.RISK_FACTOR_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_KEY := v_ftbl_stu_at_risk_rec.STUDENT_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_EVOLVE_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_EVOLVE_KEY');
            BEGIN
              SELECT STUDENT_EVOLVE_KEY
              INTO v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY
              FROM k12intel_dw.DTBL_STUDENTS_EVOLVED stu_evl
              WHERE student_key = v_ftbl_stu_at_risk_rec.STUDENT_KEY
                AND v_ftbl_stu_at_risk_rec.STUDENT_RISK_IDENTIFIED_DATE BETWEEN SYS_BEGIN_DATE AND SYS_END_DATE;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_EVOLVE_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- SCHOOL_KEY
            ---------------------------------------------------------------
            v_student_curent_school_code := NULL;

            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_KEY');
            BEGIN
              SELECT school_key, FACILITIES_KEY, stu.student_current_school_code
              INTO v_ftbl_students_at_risk_record.SCHOOL_KEY, v_ftbl_students_at_risk_record.FACILITIES_KEY, v_student_curent_school_code
              FROM k12intel_dw.dtbl_schools sch
                INNER JOIN k12intel_dw.dtbl_students stu
                    ON sch.school_code = stu.student_current_school_code
              WHERE stu.student_key = v_ftbl_stu_at_risk_rec.STUDENT_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_KEY := 0;
                v_ftbl_students_at_risk_record.FACILITIES_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'SCHOOL_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'FACILITIES_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- SCHOOL_ANNUAL_ATTRIBS_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_ANNUAL_ATTRIBS_KEY');
            BEGIN
              --SELECT school_key
              --INTO v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY
              --FROM k12intel_dw.dtbl_schools
              --WHERE school_key = v_ftbl_students_at_risk_record.SCHOOL_KEY
              --  AND school_year = v_students_at_risk_data.MARK_LOCAL_SCHOOL_YEAR;
              v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'SCHOOL_ANNUAL_ATTRIBS_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- CALENDAR_DATE_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'CALENDAR_DATE_KEY');
            BEGIN
              SELECT /*date_value, */calendar_date_key
              into v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY
              FROM k12intel_dw.dtbl_calendar_dates
              WHERE date_value = v_ftbl_stu_at_risk_rec.STUDENT_RISK_IDENTIFIED_DATE;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'CALENDAR_DATE_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- SCHOOL_DATES_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_DATES_KEY');
            BEGIN
              SELECT school_dates_key
              into v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY
              FROM k12intel_dw.dtbl_school_dates
              WHERE date_value = v_ftbl_stu_at_risk_rec.STUDENT_RISK_IDENTIFIED_DATE
                AND school_code = v_student_curent_school_code;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_IDENTIFIED_DATE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_IDENTIFIED_DATE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE := v_ftbl_stu_at_risk_rec.STUDENT_RISK_IDENTIFIED_DATE;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_IDENTIFIED_DATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_EXPIRE_DATE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_EXPIRE_DATE');
            DECLARE
              v_end_of_school_year_date K12INTEL_DW.DTBL_SCHOOL_DATES.DATE_VALUE%TYPE;
            BEGIN
                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := NULL;
                /*SELECT DATE_VALUE as END_OF_SCHOOL_YR_DATE
                INTO v_end_of_school_year_date
                FROM
                (
                 SELECT
                  LOCAL_ENROLL_DAY_IN_SCHOOL_YR
                  ,DATE_VALUE
                  ,ROW_NUMBER() OVER (PARTITION BY SCHOOL_CODE ORDER BY LOCAL_ENROLL_DAY_IN_SCHOOL_YR DESC, DATE_VALUE DESC) ROW_NUM
                 FROM
                  K12INTEL_DW.DTBL_SCHOOL_DATES
                 WHERE
                  SCHOOL_CODE = v_student_curent_school_code
                  AND ROLLING_LOCAL_SCHOOL_YR_NUMBER = 0
                ) a
                WHERE ROW_NUM = 1;*/
                select end_enroll_date
                INTO v_end_of_school_year_date
                from K12INTEL_DW.DTBL_SCHOOL_DATES_YR_SUMMARY
                where school_code = v_student_curent_school_code
                    and sis_school_year = v_const_school_year;

                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE + v_ftbl_stu_at_risk_rec.RISK_FACTOR_DEFAULT_DURATION;

                IF (v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE > v_ftbl_stu_at_risk_rec.RISK_FACTOR_EFFECTIVE_END AND v_ftbl_stu_at_risk_rec.RISK_FACTOR_EFFECTIVE_END < v_end_of_school_year_date) THEN
                    v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_ftbl_stu_at_risk_rec.RISK_FACTOR_EFFECTIVE_END;
                ELSIF (v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE > v_end_of_school_year_date) THEN
                    v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_end_of_school_year_date;
                END IF;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_EXPIRE_DATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_STATUS
            ---------------------------------------------------------------